// Copyright (c) 2026, the Dart project authors. Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'dart:typed_data';

Uint8List mapSummaryFile(String path, int length) =>
    throw UnsupportedError('Native summary mappings unavailable');

final class SummaryArenaWriter {
  factory(String path, int capacity) =>
      throw UnsupportedError('Cannot map $capacity bytes at $path');
  int get length => 0;
  int append(Uint8List bytes) =>
      throw UnsupportedError('Native summary mappings unavailable');
  void close() {}
}
