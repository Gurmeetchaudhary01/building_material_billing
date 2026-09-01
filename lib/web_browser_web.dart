// Web-only helpers. This file is selected only when compiling Flutter Web.
import 'dart:html' as html;
import 'dart:typed_data';

bool get isSafariBrowser {
  final ua = html.window.navigator.userAgent.toLowerCase();
  final safari = ua.contains('safari');
  final chromium = ua.contains('chrome') ||
      ua.contains('crios') ||
      ua.contains('chromium') ||
      ua.contains('edg') ||
      ua.contains('fxios') ||
      ua.contains('firefox') ||
      ua.contains('opera') ||
      ua.contains('opr');
  return safari && !chromium;
}

Future<bool> openPdfInBrowserForPrint(
  Uint8List bytes,
  String filename,
) async {
  // Open a blank tab immediately while the call still has the user's
  // click/tap gesture. Safari can otherwise block a later popup after await.
  final popup = html.window.open('about:blank', '_blank');
  if (popup == null) return false;

  final blob = html.Blob(<dynamic>[bytes], 'application/pdf');
  final url = html.Url.createObjectUrlFromBlob(blob);

  popup.location.href = url;
  return true;
}

Future<bool> sharePdfInBrowser(
  Uint8List bytes,
  String filename,
) async {
  // Let share_plus handle the actual Web Share API. Returning false here
  // keeps this helper as a browser capability hook without duplicating it.
  return false;
}
