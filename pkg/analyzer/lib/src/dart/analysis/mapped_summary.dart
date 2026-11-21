// Copyright (c) 2026, the Dart project authors. Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

final _close = _sys.lookupFunction<Int32 Function(Int32), int Function(int)>(
  'close',
);
final _destructor = _sys
    .lookup<Pointer<Void>>('_dispatch_data_destructor_munmap')
    .value;
final _map = _sys
    .lookupFunction<
      Pointer<Void> Function(Pointer<Void>, Size, Int32, Int32, Int32, Int64),
      Pointer<Void> Function(Pointer<Void>, int, int, int, int, int)
    >('mmap');
final _open = _sys
    .lookupFunction<
      Int32 Function(Pointer<Utf8>, Int32, VarArgs<()>),
      int Function(Pointer<Utf8>, int)
    >('open');
final _own = _sys
    .lookupFunction<
      Pointer<Void> Function(Pointer<Void>, Size, Pointer<Void>, Pointer<Void>),
      Pointer<Void> Function(Pointer<Void>, int, Pointer<Void>, Pointer<Void>)
    >('dispatch_data_create');
final _release = _sys.lookup<NativeFinalizerFunction>('dispatch_release');
final _seek = _sys
    .lookupFunction<
      Int64 Function(Int32, Int64, Int32),
      int Function(int, int, int)
    >('lseek');
// Darwin owns the mapping through a dispatch_data_t. The external typed
// list and all its subviews keep that object alive, even after unlink.
final _sys = DynamicLibrary.open('/usr/lib/libSystem.B.dylib');
final _unmap = _sys
    .lookupFunction<
      Int32 Function(Pointer<Void>, Size),
      int Function(Pointer<Void>, int)
    >('munmap');

Uint8List mapSummaryFile(String path, int length) {
  if (length <= 0) throw ArgumentError.value(length);
  // Resolve the finalizer before taking ownership of a native resource.
  var release = _release;
  var destructor = _destructor;
  var fd = using(
    (arena) => _open(path.toNativeUtf8(allocator: arena), 0),
  );
  if (fd < 0) throw FileSystemException('Could not open summary batch', path);
  late Pointer<Void> ptr;
  try {
    if (_seek(fd, 0, 2) != length) {
      throw FileSystemException('Summary batch length mismatch', path);
    }
    ptr = _map(nullptr, length, 1, 1, fd, 0); // MAP_SHARED
  } finally {
    _close(fd);
  }
  if (ptr.address == Pointer<Void>.fromAddress(-1).address) {
    throw FileSystemException('Could not map summary batch', path);
  }
  var owner = _own(ptr, length, nullptr, destructor);
  if (owner == nullptr) {
    _unmap(ptr, length);
    throw StateError('Could not own summary mapping');
  }
  return ptr.cast<Uint8>().asTypedList(
    length,
    finalizer: release,
    token: owner,
  ).asUnmodifiableView();
}

/// One writer appends to the arena. Only readers need a resident mapping.
final class SummaryArenaWriter {
  final RandomAccessFile _file;
  final int _capacity;
  int _length = 0;

  factory(String path, int capacity) {
    if (capacity <= 0) throw ArgumentError.value(capacity);
    var file = File(path)..createSync(exclusive: true);
    var sink = file.openSync(mode: FileMode.writeOnly);
    try {
      sink.truncateSync(capacity);
      return SummaryArenaWriter._(sink, capacity);
    } catch (_) {
      sink.closeSync();
      rethrow;
    }
  }

  new _(this._file, this._capacity);
  int get length => _length;

  int append(Uint8List bytes) {
    var start = _length;
    var end = start + bytes.length;
    if (end > _capacity) throw StateError('Summary budget exhausted');
    _file.writeFromSync(bytes);
    _length = end;
    return start;
  }

  void close() => _file.closeSync();
}
