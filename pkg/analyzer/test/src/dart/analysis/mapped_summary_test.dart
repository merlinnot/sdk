// Copyright (c) 2026, the Dart project authors. Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:analyzer/dart/analysis/results.dart';
import 'package:analyzer/src/dart/analysis/analysis_context_collection.dart';
import 'package:analyzer/src/dart/analysis/byte_store.dart';
import 'package:analyzer/src/dart/analysis/linked_summary_coordinator.dart';
import 'package:analyzer/src/dart/analysis/mapped_summary.dart';
import 'package:test/test.dart';

void main() {
  group('mapped summary transport', () {
    test(
      'both readers retain immutable subviews after publication is removed',
      () async {
        var directory = await Directory.systemTemp.createTemp(
          'mapped-summary-test-',
        );
        try {
          var file = await File('${directory.path}/batch')
              .writeAsBytes([7, 11, 13]);
          expect(
            () => mapSummaryFile(file.path, 4),
            throwsA(isA<FileSystemException>()),
          );
          var bytes = mapSummaryFile(file.path, 3);
          var slice = Uint8List.sublistView(bytes, 1);
          var ready = ReceivePort();
          var reader = await Isolate.spawn(_readAfterUnlink, [
            file.path,
            ready.sendPort,
          ]);
          try {
            var result = ReceivePort();
            var gate = await ready.first as SendPort;
            await file.delete();
            gate.send(result.sendPort);
            expect(slice, [11, 13]);
            expect(() => bytes[0] = 0, throwsUnsupportedError);
            expect(await result.first, [7, 11, 13]);
            result.close();
          } finally {
            reader.kill();
            ready.close();
          }
        } finally {
          await directory.delete(recursive: true);
        }
      },
    );
    test('publishes a dependency cycle before its importing library', () async {
      var dir = await Directory.systemTemp.createTemp('analyzer-cycle-test-');
      var a = File('${dir.path}/a.dart')
        ..writeAsStringSync("import 'b.dart'; B? a;");
      var b = File('${dir.path}/b.dart')
        ..writeAsStringSync("import 'c.dart'; class B { C? c; }");
      var c = File('${dir.path}/c.dart')
        ..writeAsStringSync("import 'b.dart'; class C { B? b; }");
      var broker = LinkedSummaryBroker(
        File(Platform.resolvedExecutable).parent.parent.path,
      );
      try {
        var published = <String>[];
        expect(
          await broker.prepare(
            dir.path,
            [a.path],
            null,
            null,
            {},
            onAvailable: (cycle) => published.addAll(cycle.takePaths()),
          ),
          isTrue,
        );
        var index = published.indexOf(a.path);
        expect(published.indexOf(b.path), inInclusiveRange(0, index - 1));
        expect(published.indexOf(c.path), inInclusiveRange(0, index - 1));
      } finally {
        await broker.shutdown();
        await dir.delete(recursive: true);
      }
    });
    test('queued edits of an imported overlay and its removal', () async {
      var dir = await Directory.systemTemp.createTemp('analyzer-overlay-test-');
      var root = File('${dir.path}/a.dart')
        ..writeAsStringSync("import 'b.dart'; var value = fromB;");
      var dep = File('${dir.path}/b.dart')
        ..writeAsStringSync('const fromB = 0;');
      var broker = LinkedSummaryBroker(
        File(Platform.resolvedExecutable).parent.parent.path,
      );
      Future<List<String>> prepare(Map<String, String> overlays) async {
        var published = <String>[];
        expect(
          await broker.prepare(
            dir.path,
            [root.path],
            null,
            null,
            {},
            overlays: overlays,
            onAvailable: (batch) => published.addAll(batch.takePaths()),
          ),
          isTrue,
        );
        return published;
      }

      try {
        await prepare({});
        var first = prepare({dep.path: 'const fromB = "a";'});
        var second = prepare({dep.path: 'const fromB = false;'});
        expect(await first, contains(dep.path));
        expect(await second, contains(dep.path));
        expect(await prepare({}), contains(dep.path));
        expect(dep.readAsStringSync(), 'const fromB = 0;');
      } finally {
        await broker.shutdown();
        await dir.delete(recursive: true);
      }
    });
    test('a producer can shut down while starting', () async {
      var broker = LinkedSummaryBroker(
        File(Platform.resolvedExecutable).parent.parent.path,
      );
      await Future.wait([broker.shutdown(), broker.shutdown()]);
    });
    test(
      'readers can consume successive cycles before a request completes',
      () async {
        var dir = await Directory.systemTemp.createTemp(
          'analyzer-publisher-test-',
        );
        var file = File('${dir.path}/data');
        var arena = SummaryArenaWriter(file.path, 3);
        var scope = jsonEncode([dir.path, null, null, <String, String>{}]);
        var index = File('${dir.path}/index');
        var start = arena.append(Uint8List.fromList([3, 5]));
        var firstRecord = jsonEncode([
          [
            ['a.unlinked2', start, 1],
            ['a.linked', start + 1, 1],
          ],
          ['a'],
        ]);
        index.writeAsStringSync('${jsonEncode([scope])}\n$firstRecord\n');
        var publisher = ReceivePort();
        var client = LinkedSummaryCoordinator(publisher.sendPort);
        var store = client.wrapIfSupported(MemoryByteStore())!;
        var descriptor = [dir.path, 3, 0];
        var first = Completer<void>();
        var second = Completer<void>();
        late SendPort reply;
        publisher.listen((dynamic message) {
          if (message case [_, _, _, _, _, true, _, _, SendPort port]) {
            reply = port;
            reply.send(['progress', descriptor]);
          }
        });
        try {
          var prepared = client.prepare(
            dir.path,
            [],
            null,
            null,
            {},
            onAvailable: (publication) {
              var paths = publication.takePaths();
              if (!first.isCompleted) {
                expect(paths, ['a']);
                first.complete();
              } else if (!second.isCompleted) {
                expect(paths, ['b']);
                second.complete();
              }
            },
          );
          await first.future;
          var held = store.get('a.linked')!;
          expect(held, [5]);
          expect(store.get('b.linked'), isNull);
          var next = arena.append(Uint8List.fromList([7]));
          var secondRecord = jsonEncode([
            [
              ['b.linked', next, 1],
            ],
            ['b'],
          ]);
          index.writeAsStringSync('$secondRecord\n', mode: FileMode.append);
          reply.send(['progress', descriptor]);
          await second.future;
          expect(store.get('b.linked'), [7]);
          expect(() => arena.append(Uint8List(1)), throwsStateError);
          expect(
            () => SummaryArenaWriter(file.path, 1),
            throwsA(isA<FileSystemException>()),
          );
          reply.send(descriptor);
          expect(await prepared, isTrue);
          client.close();
          await file.delete();
          expect(held, [5]);
          expect(() => held[0] = 0, throwsUnsupportedError);
        } finally {
          arena.close();
          publisher.close();
          client.close();
          await dir.delete(recursive: true);
        }
      },
    );
    for (var (name, store, expectedRequests) in [
      ('rejected producer request', MemoryByteStore(), 1),
      ('reference-counted store', CiderByteStore(), 0),
    ]) {
      test('resolves locally with a $name', () async {
        var dir = await Directory.systemTemp.createTemp('analyzer-fallback-');
        var file = File('${dir.path}/client.dart')
          ..writeAsStringSync('int value = "wrong";');
        var inbox = ReceivePort();
        var requests = 0;
        inbox.listen((dynamic message) {
          if (message case [..., SendPort reply] when message.length > 1) {
            requests++;
            reply.send(null);
          }
        });
        var client = LinkedSummaryCoordinator(inbox.sendPort);
        var collection = AnalysisContextCollectionImpl(
          includedPaths: [dir.path],
          sdkPath: File(Platform.resolvedExecutable).parent.parent.path,
          byteStore: store,
          withFineDependencies: true,
          linkedSummaryCoordinator: client,
        );
        try {
          var session = collection.contextFor(file.path).currentSession;
          var result =
              await session.getResolvedUnit(file.path) as ResolvedUnitResult;
          expect(
            result.diagnostics.map((d) => d.diagnosticCode.lowerCaseName),
            ['invalid_assignment'],
          );
          expect(requests, expectedRequests);
        } finally {
          await collection.dispose();
          client.close();
          inbox.close();
          await dir.delete(recursive: true);
        }
      });
    }
    test('missing publication fails without exposing a pointer', () {
      expect(
        () => mapSummaryFile('/no-such-dart-analyzer-summary', 1),
        throwsA(isA<FileSystemException>()),
      );
    });
  }, skip: !Platform.isMacOS);
}

void _readAfterUnlink(List<dynamic> args) async {
  var [String path, SendPort ready] = args;
  var bytes = mapSummaryFile(path, 3);
  var gate = ReceivePort();
  ready.send(gate.sendPort);
  var result = await gate.first as SendPort;
  result.send(bytes.toList());
  gate.close();
}
