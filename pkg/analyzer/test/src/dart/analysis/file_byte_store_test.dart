// Copyright (c) 2017, the Dart project authors. Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'dart:io';
import 'dart:typed_data';

import 'package:analyzer/src/dart/analysis/file_byte_store.dart';
import 'package:test/test.dart';
import 'package:test_reflective_loader/test_reflective_loader.dart';

main() {
  test('a separate reader sees each completed synchronous write', () async {
    var dir = Directory.systemTemp.createTempSync('analyzer-byte-store-');
    try {
      var producer = FileByteStore(dir.path, synchronousWrites: true);
      var reader = FileByteStore(dir.path);
      var key = 'ab.linked';
      producer.putGet(key, Uint8List.fromList([1, 3]));
      expect(reader.get(key), [1, 3]);
      var held = reader.get(key)!;
      producer.putGet(key, Uint8List.fromList([7]));
      expect(reader.get(key), [7]);
      expect(held, [1, 3]);
      await producer.flush();
      expect(producer.pendingWriteCount, 0);
      expect(producer.failedWriteCount, 0);
    } finally {
      dir.deleteSync(recursive: true);
    }
  });
  defineReflectiveSuite(() {
    defineReflectiveTests(FileByteStoreValidatorTest);
  });
}

@reflectiveTest
class FileByteStoreValidatorTest {
  final validator = FileByteStoreValidator();

  test_get_bad_notEnoughBytes() {
    var bytes = Uint8List.fromList([1, 2, 3]);
    var data = validator.getData(bytes);
    expect(data, isNull);
  }

  test_get_bad_notEnoughBytes_zero() {
    var bytes = Uint8List.fromList([]);
    var data = validator.getData(bytes);
    expect(data, isNull);
  }

  test_get_bad_wrongChecksum() {
    var data = Uint8List.fromList([1, 2, 3]);
    var bytes = validator.wrapData(data);

    // Damage the checksum.
    expect(bytes[bytes.length - 1], isNot(42));
    bytes[bytes.length - 1] = 42;

    var data2 = validator.getData(bytes);
    expect(data2, isNull);
  }

  test_get_bad_wrongVersion() {
    var bytes = Uint8List.fromList([
      0xBA,
      0xDA,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
    ]);
    var data = validator.getData(bytes);
    expect(data, isNull);
  }

  test_get_good() {
    var data = Uint8List.fromList([1, 2, 3]);
    var bytes = validator.wrapData(data);
    var data2 = validator.getData(bytes);
    expect(data2, hasLength(3));
    expect(data2, data);
  }

  test_get_good_zeroBytesData() {
    var data = Uint8List.fromList([]);
    var bytes = validator.wrapData(data);
    var data2 = validator.getData(bytes);
    expect(data2, hasLength(0));
    expect(data2, data);
  }
}
