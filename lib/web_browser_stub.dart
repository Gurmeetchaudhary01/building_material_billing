import 'dart:typed_data';

bool get isSafariBrowser => false;

Future<bool> openPdfInBrowserForPrint(Uint8List bytes, String filename) async => false;

Future<bool> sharePdfInBrowser(Uint8List bytes, String filename) async => false;
