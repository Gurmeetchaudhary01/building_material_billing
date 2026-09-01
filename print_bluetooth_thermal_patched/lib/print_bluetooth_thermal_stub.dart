import 'print_bluetooth_thermal.dart';
class PrintBluetoothThermalWindows {
  static Future<void> initialize() async {}

  static Future<List<BluetoothInfo>> getPariedBluetoohts() async => [];

  static Future<bool> connect({required String macAddress}) async => false;

  static Future<bool> writeBytes({required List<int> bytes}) async => false;

  static Future<bool> disconnect() async => true;

  static bool get connectionStatus => false;

  static Future<void> notImplemented() async {}
}
