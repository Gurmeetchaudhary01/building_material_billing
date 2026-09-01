import 'package:flutter_test/flutter_test.dart';
import 'package:building_material_billing/main.dart';

void main() {
  testWidgets('Shop Billing Pro loads', (WidgetTester tester) async {
    await tester.pumpWidget(const BuildingMaterialApp());

    expect(
      find.text('Gurmeet Building Material'),
      findsOneWidget,
    );

    expect(
      find.text('NEW BILL'),
      findsOneWidget,
    );
  });
}