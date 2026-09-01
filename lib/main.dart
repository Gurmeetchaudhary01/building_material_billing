import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';
import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:firebase_core/firebase_core.dart';
import 'package:firebase_auth/firebase_auth.dart';
import 'package:cloud_firestore/cloud_firestore.dart';
import 'firebase_options.dart';
import 'package:flutter/material.dart';
import 'package:path/path.dart' as p;
import 'package:pdf/pdf.dart';
import 'package:pdf/widgets.dart' as pw;
import 'package:printing/printing.dart';
import 'package:print_bluetooth_thermal/print_bluetooth_thermal.dart';
import 'package:esc_pos_utils_plus/esc_pos_utils_plus.dart';
import 'package:share_plus/share_plus.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:sqflite/sqflite.dart';
import 'package:url_launcher/url_launcher.dart';
import 'package:unified_esc_pos_printer/unified_esc_pos_printer.dart'
as unified;
import 'package:http/http.dart' as http;
import 'web_browser_stub.dart'
    if (dart.library.html) 'web_browser_web.dart' as browser;

void main() async {
WidgetsFlutterBinding.ensureInitialized();

await Firebase.initializeApp(
options: DefaultFirebaseOptions.currentPlatform,
);

final firestore = FirebaseFirestore.instance;
try {
// cloud_firestore 6.x uses Settings.persistenceEnabled for the
// cross-platform persistent cache configuration. This must be set
// before the first Firestore operation.
firestore.settings = const Settings(
persistenceEnabled: true,
cacheSizeBytes: Settings.CACHE_SIZE_UNLIMITED,
);
} catch (_) {
// Continue if persistent cache cannot be configured on this platform.
}

try {
if (FirebaseAuth.instance.currentUser == null) {
await FirebaseAuth.instance.signInAnonymously();
}
} catch (_) {
// Continue offline if anonymous auth not available.
}

await AppDatabase.init();

await loadBillsFromDatabase();
await loadProductCatalog();

await CloudSync.setupListeners();
await CloudSync.syncAll();

runApp(const BuildingMaterialApp());
}

// ================= BUSINESS INFO CONSTANTS ===================

const String businessName = 'GURMEET BUILDING MATERIAL';
const String businessAddress1 = 'Adampur Road';
const String businessAddress2 = 'Dhawarsi';
const String businessGST = '09CFZPS8916P1ZU';
const String businessContact = '9634899603';

// ================= BUILDING MATERIAL APP =====================

class BuildingMaterialApp extends StatelessWidget {
const BuildingMaterialApp({super.key});

@override
Widget build(BuildContext context) {
return MaterialApp(
debugShowCheckedModeBanner: false,
title: businessName,
theme: ThemeData(
colorScheme: ColorScheme.fromSeed(
seedColor: Colors.indigo,
brightness: Brightness.light,
),
useMaterial3: true,
scaffoldBackgroundColor: const Color(0xfff6f7fb),
),
home: const HomeScreen(),
);
}
}

// ============================================================
// FIREBASE CLOUD SYNC with Realtime Listeners and Offline Support
// ============================================================

class CloudSync {
static final FirebaseFirestore firestore = FirebaseFirestore.instance;
static const String businessId = 'gurmeet-building-material';

static DocumentReference<Map<String, dynamic>> get business =>
firestore.collection('businesses').doc(businessId);

static CollectionReference<Map<String, dynamic>> get bills =>
business.collection('bills');

static CollectionReference<Map<String, dynamic>> get products =>
business.collection('products');
static CollectionReference<Map<String, dynamic>> get expenses =>
    business.collection('expenses');
static StreamSubscription<QuerySnapshot<Map<String, dynamic>>>? _billsListener;
static StreamSubscription<QuerySnapshot<Map<String, dynamic>>>? _productsListener;
static StreamSubscription<QuerySnapshot<Map<String, dynamic>>>? _expensesListener;
static Future<void> _expenseQueue = Future<void>.value();
static bool _listenersStarted = false;
static String? _deviceId;
static Timer? _authRetryTimer;

// Bills and products have separate queues. A product snapshot must never be
// silently skipped just because a bill sync is in progress.
static Future<void> _billQueue = Future<void>.value();
static Future<void> _productQueue = Future<void>.value();

static bool get available => FirebaseAuth.instance.currentUser != null;

static final ValueNotifier<SyncStatus> syncStatusNotifier =
ValueNotifier(SyncStatus.idle);

static Future<String> _getDeviceId() async {
if (_deviceId != null) return _deviceId!;
final prefs = await SharedPreferences.getInstance();
final existing = prefs.getString('gurmeet_sync_device_id');
if (existing != null && existing.isNotEmpty) {
_deviceId = existing;
return existing;
}
final created = _newSyncId('device');
await prefs.setString('gurmeet_sync_device_id', created);
_deviceId = created;
return created;
}

static Future<void> initialize() async {
await _getDeviceId();
}

static Future<void> setupListeners() async {
await initialize();

if (!available) {
_startAuthRecovery();
return;
}

_authRetryTimer?.cancel();
_authRetryTimer = null;

if (_listenersStarted) return;

_listenersStarted = true;

await _billsListener?.cancel();
await _productsListener?.cancel();

_billsListener = bills.snapshots(includeMetadataChanges: true).listen(
(snapshot) {
_billQueue = _billQueue.then(
(_) => _mergeBills(snapshot.docs),
).catchError((_) {
syncStatusNotifier.value = SyncStatus.error;
});
},
onError: (_) {
syncStatusNotifier.value = SyncStatus.error;
},
);

_productsListener = products.snapshots(includeMetadataChanges: true).listen(
(snapshot) {
_productQueue = _productQueue.then(
(_) => _mergeProducts(snapshot.docs),
).catchError((_) {
syncStatusNotifier.value = SyncStatus.error;
});
},
onError: (_) {
syncStatusNotifier.value = SyncStatus.error;
},
);
_expensesListener = expenses.snapshots(includeMetadataChanges: true).listen(
      (snapshot) {
    _expenseQueue = _expenseQueue.then(
          (_) => _mergeExpenses(snapshot.docs),
    ).catchError((_) {
      syncStatusNotifier.value = SyncStatus.error;
    });
  },
  onError: (_) {
    syncStatusNotifier.value = SyncStatus.error;
  },
);
}

static void _startAuthRecovery() {
if (_authRetryTimer != null) return;

_authRetryTimer = Timer.periodic(
const Duration(seconds: 10),
(_) async {
if (available) {
_authRetryTimer?.cancel();
_authRetryTimer = null;
await setupListeners();
await syncAll();
return;
}

try {
await FirebaseAuth.instance.signInAnonymously();
} catch (_) {
// Still offline or anonymous auth is unavailable.
// Local data remains the source of truth until the
// next retry.
}
},
);
}

static Future<void> syncAll() async {
if (!available) return;
await initialize();

try {
syncStatusNotifier.value = SyncStatus.syncingBills;
final billSnapshot = await bills.get();
await _enqueueBillMerge(billSnapshot.docs);

syncStatusNotifier.value = SyncStatus.syncingProducts;
final productSnapshot = await products.get();
await _enqueueProductMerge(productSnapshot.docs);
final expenseSnapshot = await expenses.get();

await _expenseQueue.then(
      (_) => _mergeExpenses(expenseSnapshot.docs),
);
syncStatusNotifier.value = SyncStatus.idle;
} catch (_) {
syncStatusNotifier.value = SyncStatus.error;
}
}

static Future<void> _enqueueBillMerge(
List<QueryDocumentSnapshot<Map<String, dynamic>>> docs,
) async {
_billQueue = _billQueue.then((_) => _mergeBills(docs));
await _billQueue;
}

static Future<void> _enqueueProductMerge(
List<QueryDocumentSnapshot<Map<String, dynamic>>> docs,
) async {
_productQueue = _productQueue.then((_) => _mergeProducts(docs));
await _productQueue;
}

static Future<void> _mergeBills(
List<QueryDocumentSnapshot<Map<String, dynamic>>> docs,
) async {
syncStatusNotifier.value = SyncStatus.syncingBills;

final cloudIds = <String>{};

for (final doc in docs) {
Bill cloudBill;

try {
cloudBill = Bill.fromJson(
Map<String, dynamic>.from(doc.data()),
fallbackId: doc.id,
);
} catch (_) {
continue;
}

cloudIds.add(cloudBill.id);

final local = await AppDatabase.getBillBySyncId(cloudBill.id);

if (local == null) {
// This is a genuinely new cloud record.
await AppDatabase.upsertLocalBill(cloudBill);
continue;
}

if (cloudBill.updatedAtMs > local.updatedAtMs) {
// Cloud is newer: apply it locally only. Do NOT write
// it back to Firestore, otherwise we create a sync loop.
await AppDatabase.upsertLocalBill(cloudBill);
} else if (local.updatedAtMs > cloudBill.updatedAtMs) {
// Local is newer: this is a pending/offline local
// change. Re-publish it.
await _writeBill(local);
} else if (cloudBill.deviceId != local.deviceId &&
jsonEncode(cloudBill.toJson()) != jsonEncode(local.toJson())) {
// Equal timestamps are extremely unlikely, but can
// happen on two devices. Use the device ID only as a
// deterministic tie-breaker.
if (cloudBill.deviceId.compareTo(local.deviceId) >= 0) {
await AppDatabase.upsertLocalBill(cloudBill);
} else {
await _writeBill(local);
}
}
}

// A local record missing from the complete Firestore collection
// snapshot has not been permanently deleted by this app. Re-publish
// it so an offline-created bill cannot disappear.
final locals = await AppDatabase.getAllBills(includeDeleted: true);
for (final local in locals) {
if (!cloudIds.contains(local.id)) {
await _writeBill(local);
}
}

await loadBillsFromDatabase();
syncStatusNotifier.value = SyncStatus.idle;
}

static Future<void> _writeBill(Bill bill) async {
await bills.doc(bill.id).set(bill.toJson());
}
  static Future<void> _writeExpense(Expense expense) async {
    await expenses.doc(expense.id).set(expense.toMap());
  }
  static Future<void> _mergeExpenses(
      List<QueryDocumentSnapshot<Map<String, dynamic>>> docs,
      ) async {
    for (final doc in docs) {
      try {
        final expense = Expense.fromMap(
          Map<String, dynamic>.from(doc.data()),
        );

        final index = savedExpenses.indexWhere(
              (e) => e.id == expense.id,
        );

        if (index >= 0) {
          savedExpenses[index] = expense;
        } else {
          savedExpenses.add(expense);
        }
      } catch (_) {
        continue;
      }
    }

    savedExpenses.sort(
          (a, b) => b.date.compareTo(a.date),
    );

    notifyDataChanged();
  }
static Future<void> saveBill(Bill bill) async {
final now = DateTime.now().millisecondsSinceEpoch;
final updated = bill.copyWith(
id: bill.id.isEmpty ? _newSyncId('bill') : bill.id,
updatedAtMs: now,
createdAtMs: bill.createdAtMs == 0 ? now : bill.createdAtMs,
deviceId: await _getDeviceId(),
deleted: false,
deletedAtMs: null,
);

// Local-first: the UI/database is authoritative for this device's
// new local change. Firestore is then updated/queued by its SDK.
await AppDatabase.upsertLocalBill(updated);
await loadBillsFromDatabase();

if (!available) return;

try {
await _writeBill(updated);
syncStatusNotifier.value = SyncStatus.idle;
} catch (_) {
// Firestore's persistent client queue will retry when
// connectivity returns.
syncStatusNotifier.value = SyncStatus.error;
}
}

  static Future<void> saveExpense(Expense expense) async {
    final now = DateTime.now();

    final updated = Expense(
      id: expense.id.isEmpty
          ? _newSyncId('expense')
          : expense.id,
      title: expense.title,
      category: expense.category,
      amount: expense.amount,
      date: expense.date,
      createdAt: expense.createdAt,
      updatedAt: now,
    );

    final index = savedExpenses.indexWhere(
          (e) => e.id == updated.id,
    );

    if (index >= 0) {
      savedExpenses[index] = updated;
    } else {
      savedExpenses.add(updated);
    }

    savedExpenses.sort(
          (a, b) => b.date.compareTo(a.date),
    );

    notifyDataChanged();

    if (!available) return;

    try {
      await _writeExpense(updated);
      syncStatusNotifier.value = SyncStatus.idle;
    } catch (_) {
      syncStatusNotifier.value = SyncStatus.error;
    }
  }
static Future<void> deleteBill(int number) async {
final local = await AppDatabase.getBillByNumber(number);
if (local == null) return;

final now = DateTime.now().millisecondsSinceEpoch;
final deleted = local.copyWith(
updatedAtMs: now,
deviceId: await _getDeviceId(),
deleted: true,
deletedAtMs: now,
);

// Tombstone is retained locally and in Firestore. We never remove
// the cloud document immediately.
await AppDatabase.markBillDeleted(deleted);
await loadBillsFromDatabase();

if (!available) return;

try {
await _writeBill(deleted);
} catch (_) {
syncStatusNotifier.value = SyncStatus.error;
}
}

static Future<void> _mergeProducts(
List<QueryDocumentSnapshot<Map<String, dynamic>>> docs,
) async {
syncStatusNotifier.value = SyncStatus.syncingProducts;

final cloudIds = <String>{};

for (final doc in docs) {
Product cloudProduct;

try {
cloudProduct = Product.fromJson(
Map<String, dynamic>.from(doc.data()),
fallbackId: doc.id,
);
} catch (_) {
continue;
}

cloudIds.add(cloudProduct.id);

final index = productCatalog.indexWhere(
(p) => p.id == cloudProduct.id,
);

if (index < 0) {
// Do not match products by name here. Names are
// editable and are not unique identifiers.
productCatalog.add(cloudProduct);
} else {
final local = productCatalog[index];

if (cloudProduct.updatedAtMs > local.updatedAtMs) {
// Cloud-originated change: update local only.
productCatalog[index] = cloudProduct;
} else if (local.updatedAtMs > cloudProduct.updatedAtMs) {
// Local-originated/offline change: publish it.
await _writeProduct(local);
} else if (cloudProduct.deviceId != local.deviceId &&
jsonEncode(cloudProduct.toJson()) != jsonEncode(local.toJson())) {
if (cloudProduct.deviceId.compareTo(local.deviceId) >= 0) {
productCatalog[index] = cloudProduct;
} else {
await _writeProduct(local);
}
}
}
}

// Keep local products that have not reached Firestore yet.
for (final product in List<Product>.from(productCatalog)) {
if (!cloudIds.contains(product.id)) {
await _writeProduct(product);
}
}

await saveProductCatalog();
notifyDataChanged();
syncStatusNotifier.value = SyncStatus.idle;
}

static Future<void> _writeProduct(Product product) async {
if (product.id.isEmpty) return;
await products.doc(product.id).set(product.toJson());
}

static Future<void> saveProduct(Product product) async {
final now = DateTime.now().millisecondsSinceEpoch;
final id = product.id.isEmpty ? _newSyncId('product') : product.id;

final updated = product.copyWith(
id: id,
updatedAtMs: now,
createdAtMs: product.createdAtMs == 0 ? now : product.createdAtMs,
deviceId: await _getDeviceId(),
deleted: false,
deletedAtMs: null,
);

final index = productCatalog.indexWhere((p) => p.id == id);

if (index >= 0) {
productCatalog[index] = updated;
} else {
productCatalog.add(updated);
}

productCatalog.sort(
(a, b) => a.name.toLowerCase().compareTo(b.name.toLowerCase()),
);

await saveProductCatalog();
notifyDataChanged();

if (!available) return;

try {
await _writeProduct(updated);
syncStatusNotifier.value = SyncStatus.idle;
} catch (_) {
syncStatusNotifier.value = SyncStatus.error;
}
}

static Future<void> deleteProduct(Product product) async {
if (product.id.isEmpty) return;

final now = DateTime.now().millisecondsSinceEpoch;
final deleted = product.copyWith(
updatedAtMs: now,
deviceId: await _getDeviceId(),
deleted: true,
deletedAtMs: now,
);

final index = productCatalog.indexWhere((p) => p.id == product.id);
if (index >= 0) {
productCatalog[index] = deleted;
}

await saveProductCatalog();
notifyDataChanged();

if (!available) return;

try {
await _writeProduct(deleted);
} catch (_) {
syncStatusNotifier.value = SyncStatus.error;
}
}

static Future<void> dispose() async {
await _billsListener?.cancel();
await _productsListener?.cancel();
_billsListener = null;
_productsListener = null;
_listenersStarted = false;
_authRetryTimer?.cancel();
_authRetryTimer = null;
}
}

String _newSyncId(String prefix) {
final stamp = DateTime.now().microsecondsSinceEpoch.toRadixString(36);
final random = Random().nextInt(0xFFFFFF).toRadixString(36);
return '$prefix-$stamp-$random';
}


enum SyncStatus { idle, syncingBills, syncingProducts, error }

// ============================================================
// DATABASE with local sqflite and fallback to SharedPreferences on Web
// ============================================================

class AppDatabase {
static Database? _database;
static const String _webBillsKey = 'gurmeet_building_material_bills';

static Future<void> init() async {
if (kIsWeb) return;
final databasesPath = await getDatabasesPath();
final path = p.join(databasesPath, 'gurmeet_building_material.db');
_database = await openDatabase(
path,
version: 5,
onCreate: (db, version) async {
await db.execute('''
          CREATE TABLE bills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_id TEXT NOT NULL UNIQUE,
            number INTEGER NOT NULL UNIQUE,
            customer_name TEXT NOT NULL,
            customer_mobile TEXT NOT NULL,
            date TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            device_id TEXT NOT NULL DEFAULT '',
            deleted INTEGER NOT NULL DEFAULT 0,
            deleted_at INTEGER,
            print_customer_details INTEGER NOT NULL DEFAULT 0,
            discount REAL NOT NULL DEFAULT 0
          )
        ''');
await db.execute('''
          CREATE TABLE bill_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bill_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            unit TEXT NOT NULL,
            quantity REAL NOT NULL,
            rate REAL NOT NULL,
            FOREIGN KEY (bill_id) REFERENCES bills(id)
          )
        ''');
},
onUpgrade: (db, oldVersion, newVersion) async {
if (oldVersion < 2) {
await db.execute('ALTER TABLE bills ADD COLUMN sync_id TEXT');
await db.execute('ALTER TABLE bills ADD COLUMN updated_at INTEGER');
await db.execute('ALTER TABLE bills ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0');

final rows = await db.query(
'bills',
columns: ['id', 'number'],
);
final now = DateTime.now().millisecondsSinceEpoch;

for (final row in rows) {
await db.update(
'bills',
{
'sync_id': 'legacy-${row['number']}',
'updated_at': now,
},
where: 'id = ?',
whereArgs: [row['id']],
);
}

await db.execute(
'CREATE UNIQUE INDEX IF NOT EXISTS idx_bills_sync_id ON bills(sync_id)',
);
}

if (oldVersion < 3) {
await db.execute(
"ALTER TABLE bills ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0",
);
await db.execute(
"ALTER TABLE bills ADD COLUMN device_id TEXT NOT NULL DEFAULT ''",
);
await db.execute(
"ALTER TABLE bills ADD COLUMN deleted_at INTEGER",
);

final rows = await db.query(
'bills',
columns: ['id', 'updated_at'],
);

for (final row in rows) {
await db.update(
'bills',
{
'created_at':
(row['updated_at'] as num?)?.toInt() ?? 0,
},
where: 'id = ?',
whereArgs: [row['id']],
);
}
}

if (oldVersion < 4) {
await db.execute(
"ALTER TABLE bills ADD COLUMN print_customer_details INTEGER NOT NULL DEFAULT 0",
);
}


  if (oldVersion < 5) {
    await db.execute(
"ALTER TABLE bills ADD COLUMN discount REAL NOT NULL DEFAULT 0",
   );
}
},
);
}

static Database get db => _database!;

static Future<int> getNextBillNumber() async {
final all = await getAllBills(includeDeleted: true);
if (all.isEmpty) return 1;
return all.map((b) => b.number).reduce(max) + 1;
}

static Future<Bill?> getBillBySyncId(String syncId) async {
if (kIsWeb) {
final all = await getAllBills(includeDeleted: true);
for (final b in all) { if (b.id == syncId) return b; }
return null;
}
final rows = await db.query('bills', where: 'sync_id = ?', whereArgs: [syncId], limit: 1);
return rows.isEmpty ? null : _readBillRow(rows.first);
}

static Future<Bill?> getBillByNumber(int number) async {
final all = await getAllBills();
for (final b in all) { if (b.number == number) return b; }
return null;
}

static Future<void> insertBill(Bill bill) async {
await upsertLocalBill(bill);
}

static Future<void> updateBill(Bill bill) async {
await upsertLocalBill(bill);
}

static Future<void> upsertLocalBill(Bill bill) async {
final normalized =
bill.id.isEmpty ? bill.copyWith(id: _newSyncId('bill')) : bill;

if (kIsWeb) {
final prefs = await SharedPreferences.getInstance();
final raw = prefs.getString(_webBillsKey);
final list = raw == null || raw.isEmpty
? <dynamic>[]
    : jsonDecode(raw) as List<dynamic>;

final index = list.indexWhere(
(e) =>
Map<String, dynamic>.from(e as Map)['id'] ==
normalized.id,
);

if (index >= 0) {
list[index] = normalized.toJson();
} else {
list.add(normalized.toJson());
}

await prefs.setString(
_webBillsKey,
jsonEncode(list),
);
return;
}

await db.transaction((tx) async {
final existing = await tx.query(
'bills',
columns: ['id'],
where: 'sync_id = ?',
whereArgs: [normalized.id],
limit: 1,
);

int localId = existing.isEmpty
? -1
    : existing.first['id'] as int;

// Bill numbers are human-readable and remain unique locally,
// but they are NOT the Firestore document ID. If two offline
// devices independently create the same number, keep the
// incoming immutable sync ID and move the conflicting local
// bill to the next free number.
final collision = await tx.query(
'bills',
columns: ['id', 'sync_id'],
where: existing.isEmpty
? 'number = ? AND deleted = 0'
    : 'number = ? AND id != ? AND deleted = 0',
whereArgs: existing.isEmpty
? [normalized.number]
    : [normalized.number, localId],
limit: 1,
);

if (collision.isNotEmpty) {
final maxRow = await tx.rawQuery(
'SELECT MAX(number) AS max_number FROM bills',
);
final maxNumber =
(maxRow.first['max_number'] as num?)?.toInt() ??
0;

await tx.update(
'bills',
{'number': maxNumber + 1},
where: 'id = ?',
whereArgs: [collision.first['id']],
);
}

if (localId < 0) {
localId = await tx.insert(
'bills',
{
'sync_id': normalized.id,
'number': normalized.number,
'customer_name':
normalized.customerName,
'customer_mobile':
normalized.customerMobile,
'date': normalized.date
    .toIso8601String(),
'created_at':
normalized.createdAtMs,
'updated_at':
normalized.updatedAtMs,
'device_id':
normalized.deviceId,
'deleted':
normalized.deleted ? 1 : 0,
'deleted_at':
normalized.deletedAtMs,
'print_customer_details':
normalized.printCustomerDetails ? 1 : 0,
'discount': normalized.discount,
},
);
} else {
await tx.update(
'bills',
{
'number': normalized.number,
'customer_name':
normalized.customerName,
'customer_mobile':
normalized.customerMobile,
'date': normalized.date
    .toIso8601String(),
'created_at':
normalized.createdAtMs,
'updated_at':
normalized.updatedAtMs,
'device_id':
normalized.deviceId,
'deleted':
normalized.deleted ? 1 : 0,
'deleted_at':
normalized.deletedAtMs,
'print_customer_details':
normalized.printCustomerDetails ? 1 : 0,
'discount': normalized.discount,
},
where: 'id = ?',
whereArgs: [localId],
);

await tx.delete(
'bill_items',
where: 'bill_id = ?',
whereArgs: [localId],
);
}

for (final item in normalized.items) {
await tx.insert(
'bill_items',
{
'bill_id': localId,
'name': item.name,
'unit': item.unit,
'quantity': item.quantity,
'rate': item.rate,
},
);
}
});
}

static Future<Bill> upsertCloudBill(Bill bill) async {
await upsertLocalBill(bill);
return (await getBillBySyncId(bill.id)) ?? bill;
}

static Future<List<Bill>> getAllBills({bool includeDeleted = false}) async {
if (kIsWeb) {
final prefs = await SharedPreferences.getInstance();
final raw = prefs.getString(_webBillsKey);
if (raw == null || raw.isEmpty) return [];
final decoded = jsonDecode(raw) as List<dynamic>;
final result = decoded.map((e) => Bill.fromJson(Map<String, dynamic>.from(e as Map))).where((b) => includeDeleted || !b.deleted).toList();
result.sort((a, b) => a.number.compareTo(b.number));
return result;
}
final rows = await db.query('bills', where: includeDeleted ? null : 'deleted = 0', orderBy: 'number ASC');
final result = <Bill>[];
for (final row in rows) result.add(await _readBillRow(row));
return result;
}

static Future<Bill> _readBillRow(Map<String, Object?> row) async {
final billId = row['id'] as int;
final itemRows = await db.query('bill_items', where: 'bill_id = ?', whereArgs: [billId], orderBy: 'id ASC');
return Bill(
id: row['sync_id'] as String? ??
'legacy-${row['number']}',
number: row['number'] as int,
customerName: row['customer_name'] as String? ?? '',
customerMobile: row['customer_mobile'] as String? ?? '',
date: DateTime.tryParse(
row['date'] as String? ?? '',
) ??
DateTime.now(),
items: itemRows
    .map(
(item) => BillItem(
name: item['name'] as String,
unit: item['unit'] as String,
quantity:
(item['quantity'] as num).toDouble(),
rate: (item['rate'] as num).toDouble(),
),
)
    .toList(),
createdAtMs:
(row['created_at'] as num?)?.toInt() ?? 0,
updatedAtMs:
(row['updated_at'] as num?)?.toInt() ?? 0,
deviceId: row['device_id'] as String? ?? '',
deleted: (row['deleted'] as num?)?.toInt() == 1,
deletedAtMs:
(row['deleted_at'] as num?)?.toInt(),
printCustomerDetails:
(row['print_customer_details'] as num?)?.toInt() == 1,
discount:
    (row['discount'] as num?)?.toDouble() ?? 0,
);
}

static Future<void> markBillDeleted(Bill bill) async {
if (kIsWeb) {
await upsertLocalBill(bill.copyWith(deleted: true));
return;
}
await db.update(
'bills',
{
'created_at': bill.createdAtMs,
'updated_at': bill.updatedAtMs,
'device_id': bill.deviceId,
'deleted': 1,
'deleted_at': bill.deletedAtMs,
'print_customer_details': bill.printCustomerDetails ? 1 : 0,
},
where: 'sync_id = ?',
whereArgs: [bill.id],
);
}

static Future<void> applyCloudDeleteBill(String syncId) async {
final local = await getBillBySyncId(syncId);
if (local != null) await markBillDeleted(local.copyWith(deleted: true));
}

static Future<void> deleteBill(int billNumber) => CloudSync.deleteBill(billNumber);
}


// ============================================================
// BILL MODEL CLASSES
// ============================================================

class BillItem {
final String name;
final String unit;
final double quantity;
final double rate;
BillItem({required this.name, required this.unit, required this.quantity, required this.rate});
double get amount => quantity * rate;
Map<String, dynamic> toJson() => {'name': name, 'unit': unit, 'quantity': quantity, 'rate': rate};
factory BillItem.fromJson(Map<String, dynamic> json) => BillItem(
name: json['name']?.toString() ?? '', unit: json['unit']?.toString() ?? 'piece',
quantity: (json['quantity'] as num?)?.toDouble() ?? 0, rate: (json['rate'] as num?)?.toDouble() ?? 0,
);
}

class Bill {
final String id;
final int number;
final String customerName;
final String customerMobile;
final DateTime date;
final List<BillItem> items;
final int createdAtMs;
final int updatedAtMs;
final bool printCustomerDetails;
final double discount;
final String deviceId;
final bool deleted;
final int? deletedAtMs;

Bill({
String? id,
required this.number,
required this.customerName,
required this.customerMobile,
required this.date,
required this.items,
int? createdAtMs,
int? updatedAtMs,
this.deviceId = '',
this.deleted = false,
  this.printCustomerDetails = false,
  this.discount = 0,
  this.deletedAtMs,
})  : id = (id == null || id.isEmpty) ? _newSyncId('bill') : id,
createdAtMs = createdAtMs ?? DateTime.now().millisecondsSinceEpoch,
updatedAtMs = updatedAtMs ?? DateTime.now().millisecondsSinceEpoch;

double get subtotal => items.fold(
  0,
      (sum, item) => sum + item.amount,
);

double get total => max(0, subtotal - discount);

Bill copyWith({
String? id,
int? number,
String? customerName,
String? customerMobile,
DateTime? date,
List<BillItem>? items,
int? createdAtMs,
int? updatedAtMs,
String? deviceId,
bool? deleted,
  bool? printCustomerDetails,
  double? discount,
  int? deletedAtMs,
}) =>
Bill(
id: id ?? this.id,
number: number ?? this.number,
customerName: customerName ?? this.customerName,
customerMobile: customerMobile ?? this.customerMobile,
date: date ?? this.date,
items: items ?? List<BillItem>.from(this.items),
createdAtMs: createdAtMs ?? this.createdAtMs,
updatedAtMs: updatedAtMs ?? this.updatedAtMs,
deviceId: deviceId ?? this.deviceId,
deleted: deleted ?? this.deleted,
printCustomerDetails: printCustomerDetails ?? this.printCustomerDetails,
  discount: discount ?? this.discount,
deletedAtMs: deletedAtMs ?? this.deletedAtMs,
);

Map<String, dynamic> toJson() => {
'id': id,
'number': number,
'customerName': customerName,
'customerMobile': customerMobile,
'date': date.toIso8601String(),
'items': items.map((e) => e.toJson()).toList(),
'createdAtMs': createdAtMs,
'updatedAtMs': updatedAtMs,
'deviceId': deviceId,
'deleted': deleted,
  'printCustomerDetails': printCustomerDetails,
  'discount': discount,
  'deletedAtMs': deletedAtMs,
};

factory Bill.fromJson(
Map<String, dynamic> json, {
String? fallbackId,
}) =>
Bill(
id: (json['id']?.toString().isNotEmpty ?? false)
? json['id'].toString()
    : (fallbackId ?? _newSyncId('bill')),
number: (json['number'] as num?)?.toInt() ?? 0,
customerName: json['customerName']?.toString() ?? '',
customerMobile: json['customerMobile']?.toString() ?? '',
date: DateTime.tryParse(json['date']?.toString() ?? '') ??
DateTime.now(),
items: ((json['items'] as List?) ?? const [])
    .map(
(e) => BillItem.fromJson(
Map<String, dynamic>.from(e as Map),
),
)
    .toList(),
createdAtMs: (json['createdAtMs'] as num?)?.toInt() ??
(json['updatedAtMs'] as num?)?.toInt() ??
0,
updatedAtMs:
(json['updatedAtMs'] as num?)?.toInt() ?? 0,
deviceId: json['deviceId']?.toString() ?? '',
deleted: json['deleted'] == true,
printCustomerDetails: json['printCustomerDetails'] == true,
  discount: (json['discount'] as num?)?.toDouble() ?? 0,
deletedAtMs: (json['deletedAtMs'] as num?)?.toInt(),
);
}

class Expense {
  final String id;
  final String title;
  final String category;
  final double amount;
  final DateTime date;
  final DateTime createdAt;
  final DateTime updatedAt;

  Expense({
    required this.id,
    required this.title,
    required this.category,
    required this.amount,
    required this.date,
    required this.createdAt,
    required this.updatedAt,
  });

  Map<String, dynamic> toMap() {
    return {
      'id': id,
      'title': title,
      'category': category,
      'amount': amount,
      'date': date.toIso8601String(),
      'createdAt': createdAt.toIso8601String(),
      'updatedAt': updatedAt.toIso8601String(),
    };
  }

  factory Expense.fromMap(Map<String, dynamic> map) {
    return Expense(
      id: map['id']?.toString() ?? '',
      title: map['title']?.toString() ?? '',
      category: map['category']?.toString() ?? '',
      amount: (map['amount'] as num?)?.toDouble() ?? 0,
      date: DateTime.parse(map['date'].toString()),
      createdAt: DateTime.parse(map['createdAt'].toString()),
      updatedAt: DateTime.parse(map['updatedAt'].toString()),
    );
  }
}

class Product {
final String id;
final String name;
final String unit;
final double rate;
final String category;
final int createdAtMs;
final int updatedAtMs;
final String deviceId;
final bool deleted;
final int? deletedAtMs;

const Product({
this.id = '',
required this.name,
required this.unit,
required this.rate,
this.category = 'general',
this.createdAtMs = 0,
this.updatedAtMs = 0,
this.deviceId = '',
this.deleted = false,
this.deletedAtMs,
});

Product copyWith({
String? id,
String? name,
String? unit,
double? rate,
String? category,
int? createdAtMs,
int? updatedAtMs,
String? deviceId,
bool? deleted,
int? deletedAtMs,
}) =>
Product(
id: id ?? this.id,
name: name ?? this.name,
unit: unit ?? this.unit,
rate: rate ?? this.rate,
category: category ?? this.category,
createdAtMs: createdAtMs ?? this.createdAtMs,
updatedAtMs: updatedAtMs ?? this.updatedAtMs,
deviceId: deviceId ?? this.deviceId,
deleted: deleted ?? this.deleted,
deletedAtMs: deletedAtMs ?? this.deletedAtMs,
);

Map<String, dynamic> toJson() => {
'id': id,
'name': name,
'unit': unit,
'rate': rate,
'category': category,
'createdAtMs': createdAtMs,
'updatedAtMs': updatedAtMs,
'deviceId': deviceId,
'deleted': deleted,
'deletedAtMs': deletedAtMs,
};

factory Product.fromJson(
Map<String, dynamic> json, {
String? fallbackId,
}) =>
Product(
id: (json['id']?.toString().isNotEmpty ?? false)
? json['id'].toString()
    : (fallbackId ?? ''),
name: json['name']?.toString() ?? '',
unit: json['unit']?.toString() ?? 'piece',
rate: (json['rate'] as num?)?.toDouble() ?? 0,
category: json['category']?.toString() ?? 'general',
createdAtMs: (json['createdAtMs'] as num?)?.toInt() ??
(json['updatedAtMs'] as num?)?.toInt() ??
0,
updatedAtMs:
(json['updatedAtMs'] as num?)?.toInt() ?? 0,
deviceId: json['deviceId']?.toString() ?? '',
deleted: json['deleted'] == true,
deletedAtMs: (json['deletedAtMs'] as num?)?.toInt(),
);
}


// =================== PRODUCT CATALOG ========================

// Using existing defaults but managed in persistent storage.

String _stableProductId(String name) {
final value = name.trim().toLowerCase().replaceAll(RegExp(r'[^a-z0-9]+'), '-').replaceAll(RegExp(r'^-+|-+$'), '');
return 'product-$value';
}

final List<Product> productCatalog = [];

const String _productCatalogKey = 'gurmeet_building_material_products';

const List<Product> _defaultProducts = [
Product(name: 'Cement', unit: 'bag', rate: 0),
Product(name: 'Iron Sariya', unit: 'kg', rate: 0),
Product(name: 'Bricks', unit: 'piece', rate: 0),
Product(name: 'Sand', unit: 'cft', rate: 0),
Product(name: 'Bajri', unit: 'cft', rate: 0),
Product(name: 'Binding Wire', unit: 'kg', rate: 0),
Product(name: 'Nails', unit: 'kg', rate: 0),
Product(name: 'MS Angle', unit: 'kg', rate: 0),
Product(name: 'MS Channel', unit: 'kg', rate: 0),
Product(name: 'MS Sheet', unit: 'piece', rate: 0),
// ... (rest are plumbing/hardware from your original)
];

// Fetching recommended products including recently billed for smart recommendations.
List<Product> getRecommendedProducts(String query) {
final q = query.trim().toLowerCase();
if (q.isEmpty) return [];
final byName = <String, Product>{};
for (final product in productCatalog.where((p) => !p.deleted)) {
if (product.name.toLowerCase().contains(q)) {
byName[product.name.toLowerCase()] = product.copyWith(rate: getLastBilledRate(product.name) ?? product.rate);
}
}
for (final bill in savedBills.reversed) {
for (final item in bill.items.reversed) {
final key = item.name.trim().toLowerCase();
if (item.name.toLowerCase().contains(q) && !byName.containsKey(key)) {
byName[key] = Product(id: _newSyncId('recent'), name: item.name, unit: item.unit, rate: getLastBilledRate(item.name) ?? item.rate, category: 'recent', updatedAtMs: DateTime.now().millisecondsSinceEpoch);
}
}
}
return byName.values.take(10).toList();
}

double? getLastBilledRate(String productName) {
final target = productName.trim().toLowerCase();
for (final bill in savedBills.reversed) {
for (final item in bill.items.reversed) if (item.name.trim().toLowerCase() == target) return item.rate;
}
return null;
}

Future<void> loadProductCatalog() async {
final prefs = await SharedPreferences.getInstance();
final raw = prefs.getString(_productCatalogKey);

if (raw == null || raw.isEmpty) {
productCatalog
..clear()
..addAll(
_defaultProducts.map(
(p) => p.copyWith(id: _stableProductId(p.name)),
),
);
await saveProductCatalog();
notifyDataChanged();
return;
}

final decoded = jsonDecode(raw) as List<dynamic>;

productCatalog
..clear()
..addAll(
decoded.map((e) {
final product = Product.fromJson(
Map<String, dynamic>.from(e as Map),
);
return product.id.isEmpty
? product.copyWith(id: _stableProductId(product.name))
    : product;
}),
);

for (final defaultProduct in _defaultProducts) {
if (!productCatalog.any(
(p) =>
p.name.toLowerCase() ==
defaultProduct.name.toLowerCase(),
)) {
productCatalog.add(
defaultProduct.copyWith(
id: _stableProductId(defaultProduct.name),
),
);
}
}

await saveProductCatalog();
notifyDataChanged();
}

Future<void> saveProductCatalog() async {
final prefs = await SharedPreferences.getInstance();
await prefs.setString(
_productCatalogKey,
jsonEncode(productCatalog.map((p) => p.toJson()).toList()),
);
}

Future<void> upsertProduct(Product product, {int? index}) async {
// IMPORTANT: when editing an existing catalog row, retain its immutable
// ID. Creating a fresh ID here was causing the old Firestore document to
// remain while the edited product was uploaded as a second document.
String id = product.id;

if (id.isEmpty &&
index != null &&
index >= 0 &&
index < productCatalog.length) {
id = productCatalog[index].id;
}

if (id.isEmpty) {
id = _newSyncId('product');
}

final existingAtIndex =
index != null && index >= 0 && index < productCatalog.length
? productCatalog[index]
    : null;

final normalized = product.copyWith(
id: id,
createdAtMs: existingAtIndex?.createdAtMs ??
product.createdAtMs,
updatedAtMs: DateTime.now().millisecondsSinceEpoch,
deleted: false,
);

if (index != null &&
index >= 0 &&
index < productCatalog.length) {
productCatalog[index] = normalized;
} else {
final existing = productCatalog.indexWhere(
(p) =>
p.name.trim().toLowerCase() ==
normalized.name.trim().toLowerCase() &&
!p.deleted,
);

if (existing >= 0) {
final existingProduct = productCatalog[existing];
productCatalog[existing] = normalized.copyWith(
id: existingProduct.id,
createdAtMs: existingProduct.createdAtMs,
);
} else {
productCatalog.add(normalized);
}
}

productCatalog.sort(
(a, b) => a.name.toLowerCase().compareTo(b.name.toLowerCase()),
);

await saveProductCatalog();
notifyDataChanged();

// CloudSync.saveProduct preserves the ID above and uploads the same
// immutable document rather than creating a duplicate.
await CloudSync.saveProduct(
productCatalog.firstWhere((p) => p.id == id),
);
}

Future<void> deleteProductFromCatalog(int index) async {
if (index < 0 || index >= productCatalog.length) return;
await CloudSync.deleteProduct(productCatalog[index]);
}


// ====================== GLOBAL BILL DATA ========================

final ValueNotifier<int> dataRevisionNotifier = ValueNotifier<int>(0);

void notifyDataChanged() {
dataRevisionNotifier.value++;
}

final List<Bill> savedBills = [];
int nextBillNumber = 1;
final List<Expense> savedExpenses = [];

Future<void> loadBillsFromDatabase() async {
final bills = await AppDatabase.getAllBills();
savedBills
..clear()
..addAll(bills.where((bill) => !bill.deleted));

nextBillNumber = await AppDatabase.getNextBillNumber();
notifyDataChanged();
}


// ======================== PDF BILL GENERATION =======================
Future<pw.Font?> _loadRupeeFontSafely() async {
  try {
    final fontData = await rootBundle.load('assets/fonts/NotoSans-Regular.ttf');
    return pw.Font.ttf(fontData.buffer.asByteData());
  } catch (_) {
    // Printing/sharing must still work if the optional font asset is absent.
    return null;
  }
}

String _moneyText(double value, pw.Font? rupeeFont) {
  final amount = value.toStringAsFixed(2);
  return rupeeFont == null ? 'Rs. $amount' : '₹$amount';
}

pw.TextStyle _moneyStyle({
  pw.Font? font,
  double fontSize = 9,
  bool bold = false,
}) {
  return pw.TextStyle(
    font: font,
    fontSize: fontSize,
    fontWeight: bold ? pw.FontWeight.bold : pw.FontWeight.normal,
  );
}

// ======================== PDF BILL GENERATION =======================
Future<List<int>> buildBillPdf(Bill bill) async {
  final pdf = pw.Document();
  final rupeeFont = await _loadRupeeFontSafely();

  pdf.addPage(
    pw.MultiPage(
      pageFormat: PdfPageFormat.a4,
      margin: const pw.EdgeInsets.all(28),
      build: (pw.Context context) {
        return <pw.Widget>[
          pw.Column(
            crossAxisAlignment: pw.CrossAxisAlignment.stretch,
            children: [
              pw.Center(
                child: pw.Text(
                  businessName,
                  style: pw.TextStyle(
                    fontSize: 20,
                    fontWeight: pw.FontWeight.bold,
                  ),
                ),
              ),
              pw.SizedBox(height: 3),
              pw.Center(
                child: pw.Text(
                  businessAddress1,
                  style: const pw.TextStyle(fontSize: 11),
                ),
              ),
              pw.Center(
                child: pw.Text(
                  businessAddress2,
                  style: const pw.TextStyle(fontSize: 11),
                ),
              ),
              pw.SizedBox(height: 4),
              pw.Center(
                child: pw.Text(
                  'GST No: $businessGST',
                  style: const pw.TextStyle(fontSize: 10),
                ),
              ),
              pw.Center(
                child: pw.Text(
                  'Contact: $businessContact',
                  style: const pw.TextStyle(fontSize: 10),
                ),
              ),
              pw.SizedBox(height: 10),
              pw.Divider(),
              pw.Center(
                child: pw.Text(
                  'BILL',
                  style: pw.TextStyle(
                    fontSize: 15,
                    fontWeight: pw.FontWeight.bold,
                  ),
                ),
              ),
              pw.Divider(),
              pw.SizedBox(height: 8),
              pw.Row(
                mainAxisAlignment: pw.MainAxisAlignment.spaceBetween,
                crossAxisAlignment: pw.CrossAxisAlignment.start,
                children: [
                  pw.Column(
                    crossAxisAlignment: pw.CrossAxisAlignment.start,
                    children: [
                      pw.Text(
                        'Bill No: ${bill.number}',
                        style: pw.TextStyle(
                          fontSize: 10,
                          fontWeight: pw.FontWeight.bold,
                        ),
                      ),
                      pw.SizedBox(height: 3),
                      pw.Text(
                        'Date: ${bill.date.day.toString().padLeft(2, '0')}/'
                        '${bill.date.month.toString().padLeft(2, '0')}/'
                        '${bill.date.year}',
                        style: const pw.TextStyle(fontSize: 10),
                      ),
                    ],
                  ),
                  pw.Column(
                    crossAxisAlignment: pw.CrossAxisAlignment.end,
                    children: [
                      if (bill.customerName.trim().isNotEmpty)
                        pw.Text(
                          'Customer: ${bill.customerName}',
                          style: const pw.TextStyle(fontSize: 10),
                        ),
                      if (bill.customerMobile.trim().isNotEmpty)
                        pw.Text(
                          'Mobile: ${bill.customerMobile}',
                          style: const pw.TextStyle(fontSize: 10),
                        ),
                    ],
                  ),
                ],
              ),
              pw.SizedBox(height: 15),
            ],
          ),
          pw.Table(
            border: pw.TableBorder.all(width: 0.5),
            columnWidths: const {
              0: pw.FlexColumnWidth(4.2),
              1: pw.FlexColumnWidth(1.3),
              2: pw.FlexColumnWidth(1.7),
              3: pw.FlexColumnWidth(2.0),
            },
            children: [
              pw.TableRow(
                children: [
                  for (final heading in ['Item', 'Qty', 'Rate', 'Amount'])
                    pw.Padding(
                      padding: const pw.EdgeInsets.all(6),
                      child: pw.Text(
                        heading,
                        style: pw.TextStyle(
                          fontSize: 9,
                          fontWeight: pw.FontWeight.bold,
                        ),
                      ),
                    ),
                ],
              ),
              ...bill.items.map(
                (item) => pw.TableRow(
                  children: [
                    pw.Padding(
                      padding: const pw.EdgeInsets.all(6),
                      child: pw.Text(
                        item.name,
                        style: const pw.TextStyle(fontSize: 9),
                      ),
                    ),
                    pw.Padding(
                      padding: const pw.EdgeInsets.all(6),
                      child: pw.Text(
                        '${item.quantity} ${item.unit}',
                        style: const pw.TextStyle(fontSize: 9),
                      ),
                    ),
                    pw.Padding(
                      padding: const pw.EdgeInsets.all(6),
                      child: pw.Text(
                        _moneyText(item.rate, rupeeFont),
                        style: _moneyStyle(font: rupeeFont),
                      ),
                    ),
                    pw.Padding(
                      padding: const pw.EdgeInsets.all(6),
                      child: pw.Text(
                        _moneyText(item.amount, rupeeFont),
                        style: _moneyStyle(font: rupeeFont),
                      ),
                    ),
                  ],
                ),
              ),
            ],
          ),
          pw.SizedBox(height: 15),
          pw.Align(
            alignment: pw.Alignment.centerRight,
            child: pw.Column(
              crossAxisAlignment: pw.CrossAxisAlignment.end,
              children: [
                pw.Text(
                  'SUBTOTAL: ${_moneyText(bill.subtotal, rupeeFont)}',
                  style: _moneyStyle(
                    font: rupeeFont,
                    fontSize: 11,
                  ),
                ),
                if (bill.discount > 0)
                  pw.Text(
                    'DISCOUNT: -${_moneyText(bill.discount, rupeeFont)}',
                    style: _moneyStyle(
                      font: rupeeFont,
                      fontSize: 11,
                    ),
                  ),
                pw.SizedBox(height: 3),
                pw.Text(
                  'TOTAL: ${_moneyText(bill.total, rupeeFont)}',
                  style: _moneyStyle(
                    font: rupeeFont,
                    fontSize: 13,
                    bold: true,
                  ),
                ),
              ],
            ),
          ),
          pw.SizedBox(height: 20),
          pw.Center(
            child: pw.Text(
              'Thank you for shopping with us.',
              style: const pw.TextStyle(fontSize: 9),
            ),
          ),
        ];
      },
    ),
  );

  return pdf.save();
}

// ======================== THERMAL 58MM PRINT LAYOUT ========================
Future<void> print80mmBill(
  BuildContext context,
  Bill bill,
) async {
  try {
    final rupeeFont = await _loadRupeeFontSafely();
    final pdf = pw.Document();
    const thermalPageFormat = PdfPageFormat(
      80 * PdfPageFormat.mm,
      200 * PdfPageFormat.mm,
      marginLeft: 3 * PdfPageFormat.mm,
      marginRight: 3 * PdfPageFormat.mm,
      marginTop: 3 * PdfPageFormat.mm,
      marginBottom: 3 * PdfPageFormat.mm,
    );

    pdf.addPage(
      pw.MultiPage(
        pageFormat: thermalPageFormat,
        margin: const pw.EdgeInsets.all(3 * PdfPageFormat.mm),
        build: (pw.Context context) {
          return [
            pw.Center(
              child: pw.Text(
                businessName,
                textAlign: pw.TextAlign.center,
                style: pw.TextStyle(
                  fontSize: 13,
                  fontWeight: pw.FontWeight.bold,
                ),
              ),
            ),
            pw.Center(
              child: pw.Text(
                businessAddress1,
                style: const pw.TextStyle(fontSize: 9),
              ),
            ),
            pw.Center(
              child: pw.Text(
                businessAddress2,
                style: const pw.TextStyle(fontSize: 9),
              ),
            ),
            pw.Center(
              child: pw.Text(
                'GST No: $businessGST',
                style: const pw.TextStyle(fontSize: 8),
              ),
            ),
            pw.Center(
              child: pw.Text(
                'Contact: $businessContact',
                style: const pw.TextStyle(fontSize: 8),
              ),
            ),
            pw.SizedBox(height: 4),
            pw.Divider(),
            pw.Center(
              child: pw.Text(
                'BILL',
                style: pw.TextStyle(
                  fontSize: 11,
                  fontWeight: pw.FontWeight.bold,
                ),
              ),
            ),
            pw.Divider(),
            pw.Text(
              'Bill No: ${bill.number}',
              style: const pw.TextStyle(fontSize: 8),
            ),
            pw.Text(
              'Date: ${bill.date.day.toString().padLeft(2, '0')}/'
              '${bill.date.month.toString().padLeft(2, '0')}/'
              '${bill.date.year}',
              style: const pw.TextStyle(fontSize: 8),
            ),
            if (bill.printCustomerDetails && bill.customerName.trim().isNotEmpty)
              pw.Text(
                'Customer: ${bill.customerName}',
                style: const pw.TextStyle(fontSize: 8),
              ),
            if (bill.printCustomerDetails && bill.customerMobile.trim().isNotEmpty)
              pw.Text(
                'Mobile: ${bill.customerMobile}',
                style: const pw.TextStyle(fontSize: 8),
              ),
            pw.SizedBox(height: 4),
            pw.Divider(),
            pw.Row(
              children: [
                pw.Expanded(
                  flex: 5,
                  child: pw.Text(
                    'Item',
                    style: pw.TextStyle(fontSize: 8, fontWeight: pw.FontWeight.bold),
                  ),
                ),
                pw.Expanded(
                  flex: 3,
                  child: pw.Text(
                    'Qty',
                    textAlign: pw.TextAlign.right,
                    style: pw.TextStyle(fontSize: 8, fontWeight: pw.FontWeight.bold),
                  ),
                ),
                pw.Expanded(
                  flex: 3,
                  child: pw.Text(
                    'Rate',
                    textAlign: pw.TextAlign.right,
                    style: pw.TextStyle(fontSize: 8, fontWeight: pw.FontWeight.bold),
                  ),
                ),
                pw.Expanded(
                  flex: 3,
                  child: pw.Text(
                    'Amount',
                    textAlign: pw.TextAlign.right,
                    style: pw.TextStyle(fontSize: 8, fontWeight: pw.FontWeight.bold),
                  ),
                ),
              ],
            ),
            pw.Divider(),
            ...bill.items.map(
              (item) => pw.Padding(
                padding: const pw.EdgeInsets.symmetric(vertical: 2),
                child: pw.Row(
                  crossAxisAlignment: pw.CrossAxisAlignment.start,
                  children: [
                    pw.Expanded(
                      flex: 5,
                      child: pw.Text(
                        item.name,
                        maxLines: 3,
                        style: const pw.TextStyle(fontSize: 8),
                      ),
                    ),
                    pw.Expanded(
                      flex: 3,
                      child: pw.Text(
                        '${item.quantity} ${item.unit}',
                        textAlign: pw.TextAlign.right,
                        style: const pw.TextStyle(fontSize: 8),
                      ),
                    ),
                    pw.Expanded(
                      flex: 3,
                      child: pw.Text(
                        _moneyText(item.rate, rupeeFont),
                        textAlign: pw.TextAlign.right,
                        style: _moneyStyle(font: rupeeFont, fontSize: 8),
                      ),
                    ),
                    pw.Expanded(
                      flex: 3,
                      child: pw.Text(
                        _moneyText(item.amount, rupeeFont),
                        textAlign: pw.TextAlign.right,
                        style: _moneyStyle(font: rupeeFont, fontSize: 8),
                      ),
                    ),
                  ],
                ),
              ),
            ),
            pw.Divider(),
            pw.Align(
              alignment: pw.Alignment.centerRight,
              child: pw.Column(
                crossAxisAlignment: pw.CrossAxisAlignment.end,
                children: [
                  pw.Text(
                    'SUBTOTAL: ${_moneyText(bill.subtotal, rupeeFont)}',
                    style: _moneyStyle(
                      font: rupeeFont,
                      fontSize: 9,
                    ),
                  ),
                  if (bill.discount > 0)
                    pw.Text(
                      'DISCOUNT: -${_moneyText(bill.discount, rupeeFont)}',
                      style: _moneyStyle(
                        font: rupeeFont,
                        fontSize: 9,
                      ),
                    ),
                  pw.SizedBox(height: 2),
                  pw.Text(
                    'TOTAL: ${_moneyText(bill.total, rupeeFont)}',
                    style: _moneyStyle(
                      font: rupeeFont,
                      fontSize: 11,
                      bold: true,
                    ),
                  ),
                ],
              ),
            ),
            pw.SizedBox(height: 8),
            pw.Center(
              child: pw.Text(
                'THANK YOU\nVISIT AGAIN',
                textAlign: pw.TextAlign.center,
                style: const pw.TextStyle(fontSize: 8),
              ),
            ),
          ];
        },
      ),
    );

    final bytes = await pdf.save();
    if (bytes.isEmpty) {
      throw Exception('PDF generation returned empty data');
    }


    // iPhone/iPad Safari has a long-standing Flutter Web printing issue: 
    // Printing.layoutPdf() can return successfully while showing no print UI.
    // Open the generated PDF in a user-initiated tab on Safari instead.
    // Safari then exposes its native PDF Share/Print controls reliably.
    if (kIsWeb) {
      final content = StringBuffer();

      content.writeln('================================');
      content.writeln('   $businessName');
      content.writeln('   $businessAddress1');
      content.writeln('   $businessAddress2');
      content.writeln('GST No: $businessGST');
      content.writeln('Contact: $businessContact');
      content.writeln('================================');
      content.writeln('             BILL');
      content.writeln('================================');
      content.writeln('Bill No: ${bill.number}');
      content.writeln(
        'Date: ${bill.date.day.toString().padLeft(2, '0')}/'
            '${bill.date.month.toString().padLeft(2, '0')}/'
            '${bill.date.year}',
      );

      if (bill.printCustomerDetails &&
          bill.customerName.trim().isNotEmpty) {
        content.writeln('Customer: ${bill.customerName}');
      }

      if (bill.printCustomerDetails &&
          bill.customerMobile.trim().isNotEmpty) {
        content.writeln('Mobile: ${bill.customerMobile}');
      }

      content.writeln('--------------------------------');
      content.writeln('Item        Qty    Rate    Amt');
      content.writeln('--------------------------------');

      for (final item in bill.items) {
        var itemName = item.name.trim();

        if (itemName.length > 11) {
          itemName = itemName.substring(0, 11);
        }

        final qty =
        '${item.quantity.toStringAsFixed(0)} ${item.unit}'.trim();

        final qtyText =
        qty.length > 5 ? qty.substring(0, 5) : qty;

        final rate = item.rate.toStringAsFixed(0);
        final amount = item.amount.toStringAsFixed(0);

        final line =
            itemName.padRight(11) +
                qtyText.padLeft(5) +
                rate.padLeft(8) +
                amount.padLeft(8);

        content.writeln(line);
      }

      content.writeln('--------------------------------');

      if (bill.discount > 0) {
        content.writeln(
          'SUBTOTAL: Rs.${bill.subtotal.toStringAsFixed(2)}',
        );
        content.writeln(
          'DISCOUNT: -Rs.${bill.discount.toStringAsFixed(2)}',
        );
      }

      content.writeln(
        'TOTAL: Rs.${bill.total.toStringAsFixed(2)}',
      );

      content.writeln('');
      content.writeln('          THANK YOU');
      content.writeln('         VISIT AGAIN');
      content.writeln('');
      content.writeln('');
      content.writeln('');

      final response = await http.post(
        Uri.parse('http://127.0.0.1:8765/print'),
        headers: {
          'Content-Type': 'application/json',
        },
        body: jsonEncode({
          'content': content.toString(),
        }),
      );

      if (response.statusCode != 200) {
        throw Exception(
          'Print bridge error: ${response.body}',
        );
      }

      final result =
      jsonDecode(response.body) as Map<String, dynamic>;

      if (result['success'] != true) {
        throw Exception(
          result['error']?.toString() ?? 'Printer failed',
        );
      }

      if (context.mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Bill printer par send ho gaya'),
          ),
        );
      }

      return;
    }
    if (!kIsWeb) {
      // ================= BLUETOOTH =================

      final bluetoothConnected =
      await BluetoothPrinterManager.isConnected();

      if (bluetoothConnected) {
        final printerBytes =
        await buildBluetooth80mmBill(bill);

        final success =
        await PrintBluetoothThermal.writeBytes(
          printerBytes,
        );

        if (!success) {
          throw Exception(
            'Bluetooth printer par print nahi hua',
          );
        }

        return;
      }

      // ================= USB / OTG =================

      if (UsbPrinterManager.isConnected) {
        final printerBytes =
        await buildBluetooth80mmBill(bill);

        await UsbPrinterManager.printBytes(printerBytes);

        return;
      }
    }


    await Printing.layoutPdf(
      name: 'Bill_${bill.number}_80mm',
      format: thermalPageFormat,
      dynamicLayout: false,
      usePrinterSettings: false,
      forceCustomPrintPaper: true,
      onLayout: (format) async =>
          Uint8List.fromList(bytes),
    );
  } catch (e) {
    if (context.mounted) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Print error: $e'),
        ),
      );
    }
  }
}
// ============================================================
// BLUETOOTH THERMAL PRINTER MANAGER
// ============================================================

class BluetoothPrinterManager {
  static const String _printerNameKey = 'selected_bt_printer_name';
  static const String _printerAddressKey = 'selected_bt_printer_address';

  static Future<List<BluetoothInfo>> getPairedPrinters() async {
    try {
      final enabled = await PrintBluetoothThermal.bluetoothEnabled;

      if (!enabled) {
        return [];
      }

      return await PrintBluetoothThermal.pairedBluetooths;
    } catch (e) {
      debugPrint('BLUETOOTH LIST ERROR: $e');
      return [];
    }
  }

  static Future<bool> connect(BluetoothInfo printer) async {
    try {
      if (await PrintBluetoothThermal.connectionStatus) {
        await PrintBluetoothThermal.disconnect;
      }

      final connected = await PrintBluetoothThermal.connect(
        macPrinterAddress: printer.macAdress,
      );

      if (connected) {
        final prefs = await SharedPreferences.getInstance();

        await prefs.setString(
          _printerNameKey,
          printer.name,
        );

        await prefs.setString(
          _printerAddressKey,
          printer.macAdress,
        );
      }

      return connected;
    } catch (e) {
      debugPrint('BLUETOOTH CONNECT ERROR: $e');
      return false;
    }
  }

  static Future<bool> disconnect() async {
    try {
      return await PrintBluetoothThermal.disconnect;
    } catch (e) {
      debugPrint('BLUETOOTH DISCONNECT ERROR: $e');
      return false;
    }
  }

  static Future<bool> isConnected() async {
    try {
      return await PrintBluetoothThermal.connectionStatus;
    } catch (_) {
      return false;
    }
  }

  static Future<String?> getSavedPrinterName() async {
    final prefs = await SharedPreferences.getInstance();
    return prefs.getString(_printerNameKey);
  }

  static Future<String?> getSavedPrinterAddress() async {
    final prefs = await SharedPreferences.getInstance();
    return prefs.getString(_printerAddressKey);
  }
}
// ============================================================
// USB / OTG THERMAL PRINTER MANAGER
// ============================================================

class UsbPrinterManager {
  static final unified.PrinterManager _manager =
  unified.PrinterManager();

  static unified.UsbPrinterDevice? _connectedDevice;

  static Future<List<unified.UsbPrinterDevice>> getPrinters() async {
    try {
      final devices = await _manager.scanPrinters(
        timeout: const Duration(seconds: 5),
        types: {
          unified.PrinterConnectionType.usb,
        },
      );

      return devices
          .whereType<unified.UsbPrinterDevice>()
          .toList();
    } catch (e) {
      debugPrint('USB PRINTER SCAN ERROR: $e');
      return [];
    }
  }

  static Future<bool> connect(
      unified.UsbPrinterDevice printer,
      ) async {
    try {
      if (_manager.isConnected) {
        await _manager.disconnect();
      }

      await _manager.connect(printer);

      if (_manager.isConnected) {
        _connectedDevice = printer;
        return true;
      }

      _connectedDevice = null;
      return false;
    } catch (e) {
      debugPrint('USB PRINTER CONNECT ERROR: $e');
      _connectedDevice = null;
      return false;
    }
  }

  static bool get isConnected =>
      _manager.isConnected && _connectedDevice != null;

  static unified.UsbPrinterDevice? get connectedPrinter =>
      _connectedDevice;

  static Future<void> printBytes(List<int> bytes) async {
    if (!_manager.isConnected) {
      throw Exception('USB printer connected nahi hai');
    }

    await _manager.printBytes(bytes);
  }

  static Future<void> disconnect() async {
    await _manager.disconnect();
    _connectedDevice = null;
  }
}

// ========================= SHARE BILL / WHATSAPP ===========================

String? normalizeIndianMobileNumber(String input) {
  var digits = input.replaceAll(RegExp(r'[^0-9]'), '');

  if (digits.startsWith('0091') && digits.length == 14) {
    digits = digits.substring(4);
  } else if (digits.startsWith('091') && digits.length == 13) {
    digits = digits.substring(3);
  } else if (digits.startsWith('91') && digits.length == 12) {
    digits = digits.substring(2);
  }

  if (!RegExp(r'^[6-9][0-9]{9}$').hasMatch(digits)) {
    return null;
  }

  return digits;
}
Future<void> shareBillWhatsApp(BuildContext context, Bill bill) async {
final normalizedNumber =
normalizeIndianMobileNumber(bill.customerMobile.trim());

if (normalizedNumber == null) {
if (context.mounted) {
ScaffoldMessenger.of(context).showSnackBar(
const SnackBar(
content: Text('Valid 10-digit Indian mobile number add karo.'),
),
);
}
return;
}

try {
final customerName = bill.customerName.trim().isEmpty
? 'Customer'
    : bill.customerName.trim();

final message =
'Gurmeet Building Material\n'
'Bill No: ${bill.number}\n'
'Customer: $customerName\n'
'Total: ₹${bill.total.toStringAsFixed(2)}';

final phone = '91$normalizedNumber';

final whatsappUrl = Uri.parse(
'https://wa.me/$phone?text=${Uri.encodeComponent(message)}',
);

final launched = await launchUrl(
whatsappUrl,
mode: LaunchMode.externalApplication,
);

if (launched && context.mounted) {
ScaffoldMessenger.of(context).showSnackBar(
const SnackBar(content: Text('WhatsApp opened for the customer.')),
);
} else if (!launched && context.mounted) {
ScaffoldMessenger.of(context).showSnackBar(
const SnackBar(
content: Text(
'WhatsApp open nahi ho saka. WhatsApp installed hai ya nahi check karo.',
),
),
);
}
} catch (e) {
debugPrint('WHATSAPP DIRECT ERROR: $e');

if (context.mounted) {
ScaffoldMessenger.of(context).showSnackBar(
SnackBar(
content: Text('WhatsApp open nahi ho saka: $e'),
),
);
}
}
}


Future<void> shareBillPdf(BuildContext context, Bill bill) async {
  try {
    final pdfBytes = await buildBillPdf(bill);
    if (pdfBytes.isEmpty) {
      throw Exception('PDF generation returned empty data');
    }

    final filename = 'Gurmeet_Building_Material_Bill_${bill.number}.pdf';
    final file = XFile.fromData(
      Uint8List.fromList(pdfBytes),
      mimeType: 'application/pdf',
    );

    // IMPORTANT: do not pass text together with the PDF. Some Web Share
    // targets (including WhatsApp) prefer the text payload and drop the file.
    // File-only sharing gives WhatsApp/Safari the best chance to receive the
    // actual PDF document.
    final result = await SharePlus.instance.share(
      ShareParams(
        files: <XFile>[file],
        subject: 'Bill #${bill.number}',
        title: 'Bill #${bill.number} PDF',
        fileNameOverrides: <String>[filename],
        downloadFallbackEnabled: true,
      ),
    );

    if (context.mounted) {
      if (result.status == ShareResultStatus.success) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(content: Text('PDF share ho gaya.')),
        );
      } else if (result.status == ShareResultStatus.unavailable) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('PDF download/share option open ho gaya.'),
          ),
        );
      }
    }
  } catch (e, stackTrace) {
    debugPrint('PDF SHARE ERROR: $e');
    debugPrintStack(stackTrace: stackTrace);

    if (context.mounted) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('PDF share failed: $e')),
      );
    }
  }
}

Future<void> showBillShareOptions(
BuildContext context,
Bill bill,
) async {
await showModalBottomSheet<void>(
context: context,
showDragHandle: true,
builder: (sheetContext) {
return SafeArea(
child: Column(
mainAxisSize: MainAxisSize.min,
children: [
const Padding(
padding: EdgeInsets.fromLTRB(20, 8, 20, 12),
child: Align(
alignment: Alignment.centerLeft,
child: Text(
'Share Bill',
style: TextStyle(
fontSize: 20,
fontWeight: FontWeight.bold,
),
),
),
),
ListTile(
leading: const Icon(Icons.chat, color: Colors.green),
title: const Text('WhatsApp Customer'),
subtitle: Text(
bill.customerMobile.trim().isEmpty
? 'Customer mobile number missing'
    : bill.customerMobile.trim(),
),
onTap: () async {
Navigator.of(sheetContext).pop();
await shareBillWhatsApp(context, bill);
},
),
ListTile(
leading: const Icon(Icons.picture_as_pdf),
title: const Text('Share PDF'),
subtitle: const Text(
'PDF ko WhatsApp, Mail, AirDrop etc. par share karo',
),
onTap: () async {
Navigator.of(sheetContext).pop();
await shareBillPdf(context, bill);
},
),
const SizedBox(height: 12),
],
),
);
},
);
}

// =================== PRINT BILL ================================

Future<void> printBill(
  BuildContext context,
  Bill bill,
) async {
  try {
    final bytes = await buildBillPdf(bill);
    if (bytes.isEmpty) {
      throw Exception('PDF generation returned empty data');
    }
    await Printing.layoutPdf(
      name: 'Bill_${bill.number}_A4',
      format: PdfPageFormat.a4,
      dynamicLayout: false,
      onLayout: (format) async => Uint8List.fromList(bytes),
    );
  } catch (e, stackTrace) {
    debugPrint('A4 PRINT ERROR: $e');
    debugPrintStack(stackTrace: stackTrace);
    if (context.mounted) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Print failed: $e')),
      );
    }
  }
}

// =================== GLOBAL LOAD/REFRESH =======================

Future<void> runCloudSync() async {
await CloudSync.syncAll();
await loadBillsFromDatabase();
await loadProductCatalog();
}

// ============================================================
// HOME SCREEN with SYNC STATUS Indicator & UI Improvements
// ============================================================

class HomeScreen extends StatefulWidget {

const HomeScreen({super.key});

@override
State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {

late final ValueNotifier<SyncStatus> _syncStatus;

@override
void initState() {
super.initState();
_syncStatus = CloudSync.syncStatusNotifier;
dataRevisionNotifier.addListener(_onDataChanged);
}

void _onDataChanged() {
if (mounted) setState(() {});
}

@override
void dispose() {
dataRevisionNotifier.removeListener(_onDataChanged);
super.dispose();
}

bool _isSameDay(DateTime a, DateTime b) {
return a.year == b.year && a.month == b.month && a.day == b.day;
}

String _formatDate(DateTime date) {
return '${date.day.toString().padLeft(2, '0')}-'
'${date.month.toString().padLeft(2, '0')}-'
'${date.year}';
}

List<Bill> get todayBills {
final now = DateTime.now();
final bills = savedBills.where((bill) => _isSameDay(bill.date, now)).toList();
bills.sort((a, b) => b.date.compareTo(a.date));
return bills;
}

double get todaySales => todayBills.fold(0, (sum, bill) => sum + bill.total);

Future<void> openNewBill() async {
await Navigator.push(
context,
MaterialPageRoute(builder: (_) => const NewBillScreen()),
);
await loadBillsFromDatabase();
await runCloudSync();
if (mounted) setState(() {});
}

String _syncStatusText(SyncStatus status) {
switch (status) {
case SyncStatus.syncingBills:
return 'Syncing bills...';
case SyncStatus.syncingProducts:
return 'Syncing products...';
case SyncStatus.error:
return 'Sync error';
case SyncStatus.idle:
return 'Synced';
}
}

Color _syncStatusColor(SyncStatus status) {
switch (status) {
case SyncStatus.error:
return Colors.redAccent;
case SyncStatus.syncingBills:
case SyncStatus.syncingProducts:
return Colors.orangeAccent;
case SyncStatus.idle:
return Colors.green;
}
}
  double get todaysExpenses {
    final now = DateTime.now();

    return savedExpenses
        .where(
          (e) =>
      e.date.year == now.year &&
          e.date.month == now.month &&
          e.date.day == now.day,
    )
        .fold(0.0, (sum, e) => sum + e.amount);
  }

  double get todaysProfit => todaySales - todaysExpenses;
@override
Widget build(BuildContext context) {
final todaysBills = todayBills.take(5).toList();

return Scaffold(
appBar: AppBar(
title: LayoutBuilder(
builder: (context, constraints) {
final width = MediaQuery.sizeOf(context).width;
return Text(
businessName,
maxLines: 1,
overflow: TextOverflow.ellipsis,
style: TextStyle(
fontWeight: FontWeight.bold,
fontSize: width < 380 ? 15 : (width < 600 ? 17 : 20),
),
);
},
),
centerTitle: false,
actions: [
  IconButton(
    tooltip: 'Bluetooth Printer',
    icon: const Icon(Icons.print_outlined),
    onPressed: () async {
      await Navigator.push(
        context,
        MaterialPageRoute(
          builder: (_) => const BluetoothPrinterScreen(),
        ),
      );

      if (mounted) {
        setState(() {});
      }
    },
  ),
  IconButton(
    tooltip: 'USB / OTG Printer',
    icon: const Icon(Icons.usb),
    onPressed: () async {
      await Navigator.push(
        context,
        MaterialPageRoute(
          builder: (_) => const UsbPrinterScreen(),
        ),
      );

      if (mounted) {
        setState(() {});
      }
    },
  ),
IconButton(
tooltip: 'Sync Data',
icon: const Icon(Icons.sync),
onPressed: () async {
await runCloudSync();
if (mounted) {
setState(() {});
ScaffoldMessenger.of(context).showSnackBar(
SnackBar(
content: Text(
CloudSync.syncStatusNotifier.value == SyncStatus.error
? 'Data sync failed'
    : 'Data sync complete',
),
),
);
}
},
),
IconButton(
tooltip: 'Products',
icon: const Icon(Icons.category_outlined),
onPressed: () async {
await Navigator.push(
context,
MaterialPageRoute(builder: (_) => const ProductCatalogScreen()),
);
if (mounted) setState(() {});
},
),
ValueListenableBuilder<SyncStatus>(
valueListenable: _syncStatus,
builder: (_, status, __) {
final narrow = MediaQuery.sizeOf(context).width < 500;
return Padding(
padding: const EdgeInsets.symmetric(horizontal: 8),
child: narrow
? Tooltip(
message: _syncStatusText(status),
child: Icon(
status == SyncStatus.error
? Icons.cloud_off
    : status == SyncStatus.idle
? Icons.cloud_done
    : Icons.cloud_sync,
color: _syncStatusColor(status),
size: 21,
),
)
    : Center(
child: Text(
_syncStatusText(status),
maxLines: 1,
overflow: TextOverflow.ellipsis,
style: TextStyle(
color: _syncStatusColor(status),
fontWeight: FontWeight.bold,
fontSize: 13,
),
),
),
);
},
),
],
),
body: SingleChildScrollView(
padding: const EdgeInsets.all(16),
child: Column(
crossAxisAlignment: CrossAxisAlignment.start,
children: [
  Container(
    width: double.infinity,
    padding: const EdgeInsets.all(22),
    decoration: BoxDecoration(
      color: const Color(0xff253f3a),
      borderRadius: BorderRadius.circular(20),
    ),
    child: Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        const Center(
          child: Text(
            'TOTAL',
            style: TextStyle(
              color: Colors.white70,
              fontSize: 14,
              fontWeight: FontWeight.w600,
            ),
          ),
        ),

        const SizedBox(height: 6),

        Center(
          child: Text(
            '₹${todaySales.toStringAsFixed(2)}',
            style: const TextStyle(
              color: Colors.white,
              fontSize: 34,
              fontWeight: FontWeight.bold,
            ),
          ),
        ),

        const SizedBox(height: 14),

        const Divider(
          color: Colors.white30,
        ),

        const SizedBox(height: 8),

        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            const Text(
              "TODAY'S SALES",
              style: TextStyle(
                color: Colors.white70,
                fontSize: 14,
                fontWeight: FontWeight.w600,
              ),
            ),
            Text(
              '₹${todaySales.toStringAsFixed(2)}',
              style: const TextStyle(
                color: Colors.white,
                fontSize: 18,
                fontWeight: FontWeight.bold,
              ),
            ),
          ],
        ),

        const SizedBox(height: 12),

        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            const Text(
              "TODAY'S EXPENSES",
              style: TextStyle(
                color: Colors.white70,
                fontSize: 14,
                fontWeight: FontWeight.w600,
              ),
            ),
            Text(
              '₹${todaysExpenses.toStringAsFixed(2)}',
              style: const TextStyle(
                color: Colors.white,
                fontSize: 18,
                fontWeight: FontWeight.bold,
              ),
            ),
          ],
        ),

        const SizedBox(height: 10),

        const Divider(
          color: Colors.white30,
        ),

        const SizedBox(height: 8),

        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            const Text(
              "TODAY'S PROFIT",
              style: TextStyle(
                color: Colors.white,
                fontSize: 17,
                fontWeight: FontWeight.bold,
              ),
            ),
            Text(
              '₹${todaysProfit.toStringAsFixed(2)}',
              style: const TextStyle(
                color: Colors.white,
                fontSize: 22,
                fontWeight: FontWeight.bold,
              ),
            ),
          ],
        ),

        const SizedBox(height: 8),

        Center(
          child: Text(
            '${todayBills.length} bills today • ${_formatDate(DateTime.now())}',
            style: const TextStyle(
              color: Colors.white54,
              fontSize: 12,
            ),
          ),
        ),
      ],
    ),
  ),

const SizedBox(height: 18),
SizedBox(
width: double.infinity,
height: 58,
child: ElevatedButton.icon(
onPressed: openNewBill,
icon: const Icon(Icons.receipt_long),
label: const Text(
'NEW BILL',
style: TextStyle(fontSize: 17, fontWeight: FontWeight.bold),
),
),
),
const SizedBox(height: 12),
SizedBox(
width: double.infinity,
height: 52,
child: OutlinedButton.icon(
onPressed: () async {
await Navigator.push(
context,
MaterialPageRoute(builder: (_) => const BillHistoryScreen()),
);
await loadBillsFromDatabase();
if (mounted) setState(() {});
},
icon: const Icon(Icons.history),
label: const Text('Bill History', style: TextStyle(fontSize: 16)),
),
),
    const SizedBox(height: 10),

    SizedBox(
      width: double.infinity,
      height: 52,
      child: OutlinedButton.icon(
        onPressed: () async {
          await Navigator.push(
            context,
            MaterialPageRoute(
              builder: (_) => const ExpenseScreen(),
            ),
          );

          if (mounted) {
            setState(() {});
          }
        },
        icon: const Icon(Icons.account_balance_wallet_outlined),
        label: const Text(
          "TODAY'S EXPENSES",
          style: TextStyle(
            fontSize: 16,
            fontWeight: FontWeight.bold,
          ),
        ),
      ),
    ),
const SizedBox(height: 28),
const Text(
"Today's Recent Bills",
style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold),
),
const SizedBox(height: 5),
Text(
_formatDate(DateTime.now()),
style: const TextStyle(color: Colors.black54, fontSize: 13),
),
const SizedBox(height: 10),
if (todaysBills.isEmpty)
Container(
width: double.infinity,
padding: const EdgeInsets.all(24),
decoration: BoxDecoration(
color: Colors.white,
borderRadius: BorderRadius.circular(16),
),
child: const Column(
children: [
Icon(Icons.receipt_long, size: 45, color: Colors.grey),
SizedBox(height: 10),
Text(
'No bills today',
style: TextStyle(color: Colors.grey, fontSize: 16),
),
],
),
)
else
...todaysBills.map(
(bill) => Card(
child: ListTile(
onTap: () async {
await Navigator.push(
context,
MaterialPageRoute(builder: (_) => NewBillScreen(editBill: bill)),
);
await loadBillsFromDatabase();
if (mounted) setState(() {});
},
leading: CircleAvatar(child: Text('${bill.number}')),
title: Text(
bill.customerName.isEmpty ? 'Walk-in Customer' : bill.customerName,
maxLines: 1,
overflow: TextOverflow.ellipsis,
),
subtitle: Text(
'${bill.items.length} items • ${bill.customerMobile.isEmpty ? 'No mobile' : bill.customerMobile}',
maxLines: 1,
overflow: TextOverflow.ellipsis,
),
trailing: SizedBox(
width: 124,
child: Row(
mainAxisAlignment: MainAxisAlignment.end,
children: [
Flexible(
child: Text(
'₹${bill.total.toStringAsFixed(2)}',
maxLines: 1,
overflow: TextOverflow.ellipsis,
textAlign: TextAlign.right,
style: const TextStyle(fontWeight: FontWeight.bold, fontSize: 14),
),
),
IconButton(
constraints: const BoxConstraints.tightFor(width: 40, height: 40),
padding: EdgeInsets.zero,
tooltip: 'Print 58mm',
icon: const Icon(Icons.print, size: 21),
onPressed: () => print80mmBill(context, bill),
),
IconButton(
constraints: const BoxConstraints.tightFor(width: 40, height: 40),
padding: EdgeInsets.zero,
tooltip: 'Share Bill',
icon: const Icon(Icons.share, size: 21),
onPressed: () => showBillShareOptions(context, bill),
),
],
),
),
),
),
),
],
),
),
);
}
}

// ============================================================
// NEW BILL SCREEN with UX and safety improvements
// ============================================================

Future<Bill?> saveAndReturnBill(Bill bill) async {
await CloudSync.saveBill(bill);
return bill;
}

class NewBillScreen extends StatefulWidget {
final Bill? editBill;

const NewBillScreen({
super.key,
this.editBill,
});

@override
State<NewBillScreen> createState() => _NewBillScreenState();
}

class _NewBillScreenState extends State<NewBillScreen> {
@override
void initState() {
super.initState();

final bill = widget.editBill;
if (bill != null) {
customerController.text = bill.customerName;
mobileController.text = bill.customerMobile;
printCustomerDetails = bill.printCustomerDetails;
discountController.text =
bill.discount == 0 ? '' : bill.discount.toStringAsFixed(2);

items.addAll(
bill.items.map(
(item) => BillItem(
name: item.name,
unit: item.unit,
quantity: item.quantity,
rate: item.rate,
),
),
);
}
}

final customerController = TextEditingController();
final mobileController = TextEditingController();
final itemController = TextEditingController();
final quantityController = TextEditingController();
final rateController = TextEditingController();
final discountController = TextEditingController();
String selectedUnit = 'kg';

final List<BillItem> items = [];
bool printCustomerDetails = false;

final List<String> units = [
'kg',
'bag',
'piece',
'ft',
'meter',
'sq ft',
'tonne',
'litre',
];

double get itemsTotal {
  return items.fold(
    0,
        (sum, item) => sum + item.amount,
  );
}

double get discountAmount {
  final value = double.tryParse(
    discountController.text.trim(),
  ) ?? 0;

  return value.clamp(0, itemsTotal).toDouble();
}

double get grandTotal {
  return itemsTotal - discountAmount;
}

Future<void> addItem() async {
final name = itemController.text.trim();

final quantity = double.tryParse(quantityController.text.trim());

final rate = double.tryParse(rateController.text.trim());

if (name.isEmpty || quantity == null || rate == null) {
if (context.mounted) {
ScaffoldMessenger.of(context).showSnackBar(
const SnackBar(
content: Text('Item name, quantity aur rate enter karo'),
),
);
}
return;
}

if (quantity <= 0 || rate < 0) {
if (context.mounted) {
ScaffoldMessenger.of(context).showSnackBar(
const SnackBar(
content: Text('Quantity/rate check karo'),
),
);
}
return;
}

// Prevent adding duplicate items with same name/unit
final indexDuplicate = items.indexWhere((element) =>
element.name.toLowerCase() == name.toLowerCase() &&
element.unit == selectedUnit);

setState(() {
if (indexDuplicate >= 0) {
// Merge quantities and update rate to latest entered rate
final existing = items[indexDuplicate];
final newQty = existing.quantity + quantity;
items[indexDuplicate] = BillItem(
name: name,
unit: selectedUnit,
quantity: newQty,
rate: rate,
);
} else {
items.add(
BillItem(
name: name,
unit: selectedUnit,
quantity: quantity,
rate: rate,
),
);
}

itemController.clear();
quantityController.clear();
rateController.clear();
});
}

void selectProduct(Product product) {
final lastRate = getLastBilledRate(product.name);
final rate = lastRate ?? product.rate;

setState(() {
itemController.text = product.name;
selectedUnit = product.unit;
rateController.text = rate == 0 ? '' : rate.toStringAsFixed(2);
});
}

Future<void> editItem(int index) async {
final item = items[index];
final nameController = TextEditingController(text: item.name);
final qtyController = TextEditingController(text: item.quantity.toString());
final rateController = TextEditingController(text: item.rate.toString());
String unit = item.unit;

final updated = await showDialog<BillItem>(
context: context,
builder: (dialogContext) {
return StatefulBuilder(
builder: (context, setDialogState) {
return AlertDialog(
title: const Text('Edit Item'),
content: SingleChildScrollView(
child: Column(
mainAxisSize: MainAxisSize.min,
children: [
TextField(
controller: nameController,
decoration: const InputDecoration(
labelText: 'Product Name',
),
),
const SizedBox(height: 10),
TextField(
controller: qtyController,
keyboardType:
const TextInputType.numberWithOptions(decimal: true),
decoration: const InputDecoration(
labelText: 'Quantity',
),
),
const SizedBox(height: 10),
DropdownButtonFormField<String>(
initialValue: units.contains(unit) ? unit : 'piece',
decoration: const InputDecoration(
labelText: 'Unit',
),
items: units
    .map(
(value) => DropdownMenuItem(
value: value,
child: Text(value),
),
)
    .toList(),
onChanged: (value) {
if (value != null) {
setDialogState(() => unit = value);
}
},
),
const SizedBox(height: 10),
TextField(
controller: rateController,
keyboardType:
const TextInputType.numberWithOptions(decimal: true),
decoration: const InputDecoration(
labelText: 'Rate',
prefixText: '₹ ',
),
),
],
),
),
actions: [
TextButton(
onPressed: () => Navigator.pop(dialogContext),
child: const Text('CANCEL'),
),
FilledButton(
onPressed: () {
final name = nameController.text.trim();
final qty = double.tryParse(qtyController.text.trim());
final rate = double.tryParse(rateController.text.trim());

if (name.isEmpty || qty == null || qty <= 0 || rate == null || rate < 0) {
return;
}

Navigator.pop(
dialogContext,
BillItem(
name: name,
unit: unit,
quantity: qty,
rate: rate,
),
);
},
child: const Text('SAVE'),
),
],
);
},
);
},
);

nameController.dispose();
qtyController.dispose();
rateController.dispose();

if (updated != null) {
setState(() {
items[index] = updated;
});
}
}

void removeItem(int index) {
setState(() {
items.removeAt(index);
});
}

bool _saving = false;

Future<void> _saveBill({required bool sendAfterSave}) async {
if (_saving) return;

if (items.isEmpty) {
if (context.mounted) {
ScaffoldMessenger.of(context).showSnackBar(
const SnackBar(
content: Text('Pehle kam se kam 1 item add karo'),
),
);
}
return;
}

final mobile = mobileController.text.trim();
final isEditing = widget.editBill != null;

if (sendAfterSave && mobile.isEmpty) {
if (context.mounted) {
ScaffoldMessenger.of(context).showSnackBar(
const SnackBar(
content: Text('SEND BILL ke liye customer mobile number enter karo'),
),
);
}
return;
}

setState(() => _saving = true);

final billNumber =
widget.editBill?.number ?? await AppDatabase.getNextBillNumber();

final bill = Bill(
id: widget.editBill?.id,
number: billNumber,
customerName: customerController.text.trim(),
customerMobile: mobile,
date: widget.editBill?.date ?? DateTime.now(),
items: List<BillItem>.from(items),
  discount: discountAmount,
createdAtMs: widget.editBill?.createdAtMs,
updatedAtMs: DateTime.now().millisecondsSinceEpoch,
deviceId: widget.editBill?.deviceId ?? '',
printCustomerDetails: printCustomerDetails,
);

try {
// CloudSync is local-first: persist locally, update the
// in-memory state, then let Firestore upload/queue the same
// immutable document ID.
await CloudSync.saveBill(bill);

if (isEditing) {
final index = savedBills.indexWhere(
(saved) => saved.id == bill.id,
);
if (index >= 0) {
savedBills[index] = bill;
} else {
savedBills.add(bill);
}
} else {
nextBillNumber = billNumber + 1;
}

notifyDataChanged();
await loadBillsFromDatabase();

if (!mounted) return;

if (sendAfterSave) {
// SEND BILL opens WhatsApp with the saved bill details.
await shareBillWhatsApp(context, bill);
if (!mounted) return;
}

final shouldPrint = await showDialog<bool>(
context: context,
builder: (dialogContext) {
return AlertDialog(
title: Text(
isEditing ? 'Bill Updated' : 'Bill Saved',
),
content: Text(
'Bill #${bill.number} ${isEditing ? 'updated' : 'saved'} successfully.\n\n'
'Total: ₹${bill.total.toStringAsFixed(2)}',
),
actions: [
TextButton(
onPressed: () => Navigator.pop(dialogContext, false),
child: const Text('DONE'),
),
FilledButton.icon(
onPressed: () => Navigator.pop(dialogContext, true),
icon: const Icon(Icons.print),
label: const Text('PRINT'),
),
],
);
},
);

if (shouldPrint == true && mounted) {
await print80mmBill(context, bill);
}

if (mounted) {
Navigator.pop(context, bill);
}
} catch (e) {
if (mounted) {
ScaffoldMessenger.of(context).showSnackBar(
SnackBar(
content: Text('Bill save/send failed: $e'),
),
);
}
} finally {
if (mounted) {
setState(() => _saving = false);
}
}
}

Future<void> saveBill() => _saveBill(sendAfterSave: false);

Future<void> sendBill() => _saveBill(sendAfterSave: true);

@override
void dispose() {
customerController.dispose();
mobileController.dispose();
itemController.dispose();
quantityController.dispose();
rateController.dispose();
discountController.dispose();

super.dispose();
}

@override
Widget build(BuildContext context) {
return Scaffold(
appBar: AppBar(
title: Text(
widget.editBill == null
? 'New Bill'
    : 'Edit Bill #${widget.editBill!.number}',
style: const TextStyle(
fontWeight: FontWeight.bold,
),
),
),
body: SingleChildScrollView(
padding: const EdgeInsets.all(16),
child: Column(
crossAxisAlignment: CrossAxisAlignment.start,
children: [
const Text(
'Customer Details',
style: TextStyle(
fontSize: 19,
fontWeight: FontWeight.bold,
),
),
const SizedBox(height: 10),
TextField(
controller: customerController,
decoration: const InputDecoration(
labelText: 'Customer Name (optional)',
prefixIcon: Icon(Icons.person),
border: OutlineInputBorder(),
),
),
const SizedBox(height: 10),
TextField(
controller: mobileController,
keyboardType: TextInputType.phone,
decoration: const InputDecoration(
labelText: 'Mobile Number (optional)',
prefixIcon: Icon(Icons.phone),
border: OutlineInputBorder(),
),
),
Padding(
padding: const EdgeInsets.only(top: 4),
child: CheckboxListTile(
contentPadding: EdgeInsets.zero,
value: printCustomerDetails,
controlAffinity: ListTileControlAffinity.leading,
title: const Text('Print Customer Details'),
onChanged: (value) {
setState(() => printCustomerDetails = value ?? false);
},
),
),
const SizedBox(height: 14),
const Text(
'Add Item',
style: TextStyle(
fontSize: 19,
fontWeight: FontWeight.bold,
),
),
const SizedBox(height: 10),
TextField(
controller: itemController,
onChanged: (_) => setState(() {}),
decoration: const InputDecoration(
labelText: 'Item Name',
hintText: 'Example: Iron Sariya',
prefixIcon: Icon(Icons.inventory_2),
border: OutlineInputBorder(),
),
),
if (itemController.text.trim().isNotEmpty) ...[
const SizedBox(height: 8),
const Text(
'Recommendations',
style: TextStyle(
fontWeight: FontWeight.w600,
color: Colors.black54,
),
),
const SizedBox(height: 6),
Builder(
builder: (context) {
final recommendations =
getRecommendedProducts(itemController.text);

if (recommendations.isEmpty) {
return const Text(
'No matching item found. Type more or use Hardware.',
style: TextStyle(color: Colors.black54),
);
}

return Wrap(
spacing: 8,
runSpacing: 8,
children: recommendations.map((product) {
final rate =
getLastBilledRate(product.name) ?? product.rate;
return ActionChip(
avatar: const Icon(
Icons.inventory_2_outlined,
size: 17,
),
label: Text(
rate > 0
? '${product.name} • ₹${rate.toStringAsFixed(2)}'
    : product.name,
),
onPressed: () => selectProduct(product),
);
}).toList(),
);
},
),
],
const SizedBox(height: 8),
SizedBox(
width: double.infinity,
height: 48,
child: OutlinedButton.icon(
onPressed: () async {
await Navigator.push(
context,
MaterialPageRoute(
builder: (_) => const ProductCatalogScreen(
categoryFilter: 'plumbing',
),
),
);
if (mounted) setState(() {});
},
icon: const Icon(Icons.plumbing_outlined),
label: const Text(
'HARDWARE / PLUMBING',
style: TextStyle(fontWeight: FontWeight.bold),
),
),
),
const SizedBox(height: 10),
Row(
children: [
Expanded(
child: TextField(
controller: quantityController,
keyboardType: const TextInputType.numberWithOptions(
decimal: true,
),
decoration: const InputDecoration(
labelText: 'Quantity',
hintText: '5',
border: OutlineInputBorder(),
),
),
),
const SizedBox(width: 10),
Expanded(
child: DropdownButtonFormField<String>(
initialValue: selectedUnit,
decoration: const InputDecoration(
labelText: 'Unit',
border: OutlineInputBorder(),
),
items: units
    .map(
(unit) => DropdownMenuItem(
value: unit,
child: Text(unit),
),
)
    .toList(),
onChanged: (value) {
if (value != null) {
setState(() {
selectedUnit = value;
});
}
},
),
),
],
),
const SizedBox(height: 10),
TextField(
controller: rateController,
keyboardType: const TextInputType.numberWithOptions(
decimal: true,
),
decoration: const InputDecoration(
labelText: 'Rate per unit',
hintText: '50',
prefixText: '₹ ',
border: OutlineInputBorder(),
),
),
const SizedBox(height: 12),
SizedBox(
width: double.infinity,
height: 52,
child: ElevatedButton.icon(
onPressed: addItem,
icon: const Icon(
Icons.add,
),
label: const Text(
'ADD ITEM',
style: TextStyle(
fontWeight: FontWeight.bold,
),
),
),
),
const SizedBox(height: 24),
if (items.isNotEmpty) ...[
const Text(
'Bill Items',
style: TextStyle(
fontSize: 19,
fontWeight: FontWeight.bold,
),
),
const SizedBox(height: 8),
...items.asMap().entries.map(
(entry) {
final index = entry.key;
final item = entry.value;

return Card(
child: ListTile(
title: Text(
item.name,
style: const TextStyle(
fontWeight: FontWeight.bold,
),
),
subtitle: Text(
'${item.quantity} ${item.unit} × '
'₹${item.rate.toStringAsFixed(2)}',
),
trailing: Row(
mainAxisSize: MainAxisSize.min,
children: [
Text(
'₹${item.amount.toStringAsFixed(2)}',
style: const TextStyle(
fontWeight: FontWeight.bold,
),
),
IconButton(
tooltip: 'Edit Item',
onPressed: () => editItem(index),
icon: const Icon(Icons.edit_outlined),
),
IconButton(
tooltip: 'Remove Item',
onPressed: () => removeItem(index),
icon: const Icon(
Icons.delete_outline,
color: Colors.red,
),
),
],
),
),
);
},
),
const SizedBox(height: 15),
  TextField(
    controller: discountController,
    keyboardType: const TextInputType.numberWithOptions(
      decimal: true,
    ),
    onChanged: (_) => setState(() {}),
    decoration: const InputDecoration(
      labelText: 'Discount',
      hintText: '20',
      prefixText: '₹ ',
      border: OutlineInputBorder(),
      helperText: 'Poore bill ke total par fixed discount',
    ),
  ),

  const SizedBox(height: 15),
  Container(
    width: double.infinity,
    padding: const EdgeInsets.all(20),
    decoration: BoxDecoration(
      color: const Color(0xff253f3a),
      borderRadius: BorderRadius.circular(18),
    ),
    child: Column(
      children: [
        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            const Text(
              'SUBTOTAL',
              style: TextStyle(
                color: Colors.white70,
                fontSize: 16,
                fontWeight: FontWeight.w600,
              ),
            ),
            Text(
              '₹${itemsTotal.toStringAsFixed(2)}',
              style: const TextStyle(
                color: Colors.white70,
                fontSize: 17,
                fontWeight: FontWeight.w600,
              ),
            ),
          ],
        ),

        if (discountAmount > 0) ...[
          const SizedBox(height: 8),
          Row(
            mainAxisAlignment: MainAxisAlignment.spaceBetween,
            children: [
              const Text(
                'DISCOUNT',
                style: TextStyle(
                  color: Colors.white70,
                  fontSize: 16,
                  fontWeight: FontWeight.w600,
                ),
              ),
              Text(
                '-₹${discountAmount.toStringAsFixed(2)}',
                style: const TextStyle(
                  color: Colors.white70,
                  fontSize: 17,
                  fontWeight: FontWeight.w600,
                ),
              ),
            ],
          ),
        ],

        const Divider(
          color: Colors.white38,
        ),

        Row(
          mainAxisAlignment: MainAxisAlignment.spaceBetween,
          children: [
            const Text(
              'TOTAL',
              style: TextStyle(
                color: Colors.white,
                fontSize: 20,
                fontWeight: FontWeight.bold,
              ),
            ),
            Text(
              '₹${grandTotal.toStringAsFixed(2)}',
              style: const TextStyle(
                color: Colors.white,
                fontSize: 25,
                fontWeight: FontWeight.bold,
              ),
            ),
          ],
        ),
      ],
    ),
  ),
  const SizedBox(height: 15),
  ],
SizedBox(
width: double.infinity,
height: 58,
child: ElevatedButton.icon(
onPressed: _saving ? null : saveBill,
icon: const Icon(
Icons.save,
),
label: const Text(
'SAVE BILL',
style: TextStyle(
fontSize: 17,
fontWeight: FontWeight.bold,
),
),
),
),
const SizedBox(height: 10),
SizedBox(
width: double.infinity,
height: 58,
child: OutlinedButton.icon(
onPressed: _saving ? null : sendBill,
icon: const Icon(Icons.send),
label: Text(
_saving ? 'SENDING...' : 'SEND BILL',
style: const TextStyle(
fontSize: 17,
fontWeight: FontWeight.bold,
),
),
),
),
],
),
),
);
}
}

// ==================== PRODUCT CATALOG SCREEN ====================
   Future<List<int>> buildBluetooth80mmBill(Bill bill) async {
  final profile = await CapabilityProfile.load();
  final generator = Generator(
    PaperSize.mm80,
    profile,
  );

  final List<int> bytes = [];

  // Header
  bytes.addAll(
    generator.text(
      businessName,
      styles: PosStyles(
        align: PosAlign.center,
        bold: true,
        height: PosTextSize.size2,
        width: PosTextSize.size2,
      ),
    ),
  );

  if (businessAddress1.trim().isNotEmpty) {
    bytes.addAll(
      generator.text(
        businessAddress1,
        styles: const PosStyles(
          align: PosAlign.center,
        ),
      ),
    );
  }

  if (businessAddress2.trim().isNotEmpty) {
    bytes.addAll(
      generator.text(
        businessAddress2,
        styles: const PosStyles(
          align: PosAlign.center,
        ),
      ),
    );
  }

  if (businessGST.trim().isNotEmpty) {
    bytes.addAll(
      generator.text(
        'GST No: $businessGST',
        styles: const PosStyles(
          align: PosAlign.center,
        ),
      ),
    );
  }

  if (businessContact.trim().isNotEmpty) {
    bytes.addAll(
      generator.text(
        'Contact: $businessContact',
        styles: const PosStyles(
          align: PosAlign.center,
        ),
      ),
    );
  }

  bytes.addAll(
    generator.text(
      '================================================',
      styles: const PosStyles(
        bold: true,
      ),
    ),
  );

  bytes.addAll(
    generator.text(
      'BILL',
      styles: PosStyles(
        align: PosAlign.center,
        bold: true,
        height: PosTextSize.size2,
      ),
    ),
  );
  bytes.addAll(
    generator.text(
      '================================================',
      styles: const PosStyles(
        bold: true,
      ),
    ),
  );

  bytes.addAll(
    generator.text(
      'Bill No: ${bill.number}',
    ),
  );

  bytes.addAll(
    generator.text(
      'Date: ${bill.date.day.toString().padLeft(2, '0')}/'
          '${bill.date.month.toString().padLeft(2, '0')}/'
          '${bill.date.year}',
    ),
  );

  if (bill.printCustomerDetails &&
      bill.customerName.trim().isNotEmpty) {
    bytes.addAll(
      generator.text(
        'Customer: ${bill.customerName}',
      ),
    );
  }

  if (bill.printCustomerDetails &&
      bill.customerMobile.trim().isNotEmpty) {
    bytes.addAll(
      generator.text(
        'Mobile: ${bill.customerMobile}',
      ),
    );
  }

  bytes.addAll(
    generator.text(
      '________________________________________________',
      styles: const PosStyles(
        bold: true,
      ),
    ),
  );

// ==================== 80MM ITEM TABLE ====================

     bytes.addAll(
       generator.text(
         'Item                    Qty      Rate       Amt',
         styles: const PosStyles(
           bold: true,
         ),
       ),
     );

     bytes.addAll(
       generator.text(
         '________________________________________________',
         styles: const PosStyles(
           bold: true,
         ),
       ),
     );

// ALL ITEMS
     for (final item in bill.items) {
       String itemName = item.name.trim();

       // 80mm printer: wider item-name space
       if (itemName.length > 20) {
         itemName = itemName.substring(0, 20);
       }

       final qty = '${item.quantity.toStringAsFixed(0)} ${item.unit}'.trim();

       final qtyText = qty.length > 6
           ? qty.substring(0, 6)
           : qty;

       final rate = item.rate.toStringAsFixed(0);
       final amount = item.amount.toStringAsFixed(0);

       final line =
           itemName.padRight(20) +
               qtyText.padLeft(6) +
               rate.padLeft(10) +
               amount.padLeft(12);

       bytes.addAll(
         generator.text(line),
       );
     }


  bytes.addAll(
    generator.text(
      '________________________________________________',
      styles: const PosStyles(
        bold: true,
      ),
    ),
  );


  // ==================== TOTALS ====================

  if (bill.discount > 0) {
    bytes.addAll(
      generator.text(
        'SUBTOTAL: Rs.${bill.subtotal.toStringAsFixed(2)}',
        styles: const PosStyles(
          align: PosAlign.right,
        ),
      ),
    );

    bytes.addAll(
      generator.text(
        'DISCOUNT: -Rs.${bill.discount.toStringAsFixed(2)}',
        styles: const PosStyles(
          align: PosAlign.right,
        ),
      ),
    );
  }

  bytes.addAll(
    generator.text(
      'TOTAL: Rs.${bill.total.toStringAsFixed(2)}',
      styles: const PosStyles(
        align: PosAlign.right,
        bold: true,
      ),
    ),
  );


  bytes.addAll(generator.feed(2));

  bytes.addAll(
    generator.text(
      'THANK YOU',
      styles: const PosStyles(
        align: PosAlign.center,
        bold: true,
      ),
    ),
  );
  bytes.addAll(
    generator.text(
      'VISIT AGAIN',
      styles: const PosStyles(
        align: PosAlign.center,
      ),
    ),
  );


  bytes.addAll(generator.feed(3));

  // Cut if printer supports it.
  bytes.addAll(generator.cut());

  return bytes;
}
class BluetoothPrinterScreen extends StatefulWidget {
  const BluetoothPrinterScreen({super.key});

  @override
  State<BluetoothPrinterScreen> createState() =>
      _BluetoothPrinterScreenState();
}

class _BluetoothPrinterScreenState
    extends State<BluetoothPrinterScreen> {
  List<BluetoothInfo> printers = [];
  bool loading = false;
  bool connected = false;
  String? selectedAddress;

  @override
  void initState() {
    super.initState();
    _loadPrinters();
  }

  Future<void> _loadPrinters() async {
    setState(() => loading = true);

    try {
      printers =
      await BluetoothPrinterManager.getPairedPrinters();

      selectedAddress =
      await BluetoothPrinterManager.getSavedPrinterAddress();

      connected =
      await BluetoothPrinterManager.isConnected();
    } catch (e) {
      debugPrint('PRINTER SCREEN ERROR: $e');
    }

    if (mounted) {
      setState(() => loading = false);
    }
  }

  Future<void> _connect(BluetoothInfo printer) async {
    setState(() => loading = true);

    final success =
    await BluetoothPrinterManager.connect(printer);

    if (mounted) {
      setState(() {
        loading = false;
        connected = success;

        if (success) {
          selectedAddress = printer.macAdress;
        }
      });

      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text(
            success
                ? '${printer.name} connected'
                : 'Printer connect nahi hua',
          ),
        ),
      );
    }
  }

  Future<void> _disconnect() async {
    await BluetoothPrinterManager.disconnect();

    if (mounted) {
      setState(() => connected = false);

      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('Printer disconnected'),
        ),
      );
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text(
          'Bluetooth Printer',
          style: TextStyle(fontWeight: FontWeight.bold),
        ),
        actions: [
          IconButton(
            tooltip: 'Refresh',
            onPressed: loading ? null : _loadPrinters,
            icon: const Icon(Icons.refresh),
          ),
        ],
      ),
      body: loading && printers.isEmpty
          ? const Center(
        child: CircularProgressIndicator(),
      )
          : RefreshIndicator(
        onRefresh: _loadPrinters,
        child: ListView(
          padding: const EdgeInsets.all(16),
          children: [
            Card(
              child: Padding(
                padding: const EdgeInsets.all(18),
                child: Column(
                  children: [
                    Icon(
                      connected
                          ? Icons.print
                          : Icons.print_disabled,
                      size: 48,
                    ),
                    const SizedBox(height: 10),
                    Text(
                      connected
                          ? 'Printer Connected'
                          : 'No Printer Connected',
                      style: const TextStyle(
                        fontSize: 19,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    const SizedBox(height: 6),
                    Text(
                      connected
                          ? 'Ready to print 80mm bills'
                          : 'Phone Bluetooth settings mein printer pair karo',
                      textAlign: TextAlign.center,
                    ),
                    if (connected) ...[
                      const SizedBox(height: 14),
                      FilledButton.icon(
                        onPressed: _disconnect,
                        icon: const Icon(Icons.link_off),
                        label: const Text('DISCONNECT'),
                      ),
                    ],
                  ],
                ),
              ),
            ),
            const SizedBox(height: 18),
            const Text(
              'Paired Bluetooth Printers',
              style: TextStyle(
                fontSize: 19,
                fontWeight: FontWeight.bold,
              ),
            ),
            const SizedBox(height: 8),
            if (printers.isEmpty)
              const Card(
                child: Padding(
                  padding: EdgeInsets.all(20),
                  child: Text(
                    'Koi paired Bluetooth printer nahi mila.\n\n'
                        'Pehle Android Settings → Bluetooth mein '
                        'printer pair karo, phir Refresh dabao.',
                    textAlign: TextAlign.center,
                  ),
                ),
              )
            else
              ...printers.map(
                    (printer) {
                  final isSelected =
                      selectedAddress ==
                          printer.macAdress;

                  return Card(
                    child: ListTile(
                      leading: const CircleAvatar(
                        child: Icon(Icons.print),
                      ),
                      title: Text(
                        printer.name.isEmpty
                            ? 'Unknown Printer'
                            : printer.name,
                      ),
                      subtitle: Text(
                        printer.macAdress,
                      ),
                      trailing:
                      isSelected && connected
                          ? const Icon(
                        Icons.check_circle,
                        color: Colors.green,
                      )
                          : FilledButton(
                        onPressed: loading
                            ? null
                            : () =>
                            _connect(printer),
                        child:
                        const Text('CONNECT'),
                      ),
                    ),
                  );
                },
              ),
          ],
        ),
      ),
    );
  }
}

class UsbPrinterScreen extends StatefulWidget {
  const UsbPrinterScreen({super.key});

  @override
  State<UsbPrinterScreen> createState() => _UsbPrinterScreenState();
}

class _UsbPrinterScreenState extends State<UsbPrinterScreen> {
  List<unified.UsbPrinterDevice> printers = [];
  bool loading = false;
  bool connected = false;

  @override
  void initState() {
    super.initState();
    _loadPrinters();
  }

  Future<void> _loadPrinters() async {
    if (mounted) {
      setState(() => loading = true);
    }

    try {
      printers = await UsbPrinterManager.getPrinters();
      connected = UsbPrinterManager.isConnected;
    } catch (e) {
      debugPrint('USB SCREEN ERROR: $e');
    }

    if (mounted) {
      setState(() => loading = false);
    }
  }

  Future<void> _connect(
      unified.UsbPrinterDevice printer,
      ) async {
    setState(() => loading = true);

    final success =
    await UsbPrinterManager.connect(printer);

    if (mounted) {
      setState(() {
        loading = false;
        connected = success;
      });

      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text(
            success
                ? '${printer.name} connected'
                : 'USB printer connect nahi hua',
          ),
        ),
      );
    }
  }

  Future<void> _disconnect() async {
    await UsbPrinterManager.disconnect();

    if (mounted) {
      setState(() => connected = false);

      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('USB printer disconnected'),
        ),
      );
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text(
          'USB / OTG Printer',
          style: TextStyle(fontWeight: FontWeight.bold),
        ),
        actions: [
          IconButton(
            tooltip: 'Refresh',
            onPressed: loading ? null : _loadPrinters,
            icon: const Icon(Icons.refresh),
          ),
        ],
      ),
      body: loading && printers.isEmpty
          ? const Center(
        child: CircularProgressIndicator(),
      )
          : RefreshIndicator(
        onRefresh: _loadPrinters,
        child: ListView(
          padding: const EdgeInsets.all(16),
          children: [
            Card(
              child: Padding(
                padding: const EdgeInsets.all(18),
                child: Column(
                  children: [
                    Icon(
                      connected
                          ? Icons.usb
                          : Icons.usb_off,
                      size: 48,
                    ),
                    const SizedBox(height: 10),
                    Text(
                      connected
                          ? 'USB Printer Connected'
                          : 'No USB Printer Connected',
                      style: const TextStyle(
                        fontSize: 19,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    const SizedBox(height: 6),
                    Text(
                      connected
                          ? 'Ready to print 80mm bills'
                          : 'Printer ko OTG cable se phone mein connect karo',
                      textAlign: TextAlign.center,
                    ),
                    if (connected) ...[
                      const SizedBox(height: 14),
                      FilledButton.icon(
                        onPressed: _disconnect,
                        icon: const Icon(Icons.link_off),
                        label: const Text('DISCONNECT'),
                      ),
                    ],
                  ],
                ),
              ),
            ),

            const SizedBox(height: 18),

            const Text(
              'USB Printers',
              style: TextStyle(
                fontSize: 19,
                fontWeight: FontWeight.bold,
              ),
            ),

            const SizedBox(height: 8),

            if (printers.isEmpty)
              const Card(
                child: Padding(
                  padding: EdgeInsets.all(20),
                  child: Text(
                    'Koi USB printer nahi mila.\n\n'
                        'Printer ko OTG cable se Android phone '
                        'mein connect karo aur Refresh dabao.',
                    textAlign: TextAlign.center,
                  ),
                ),
              )
            else
              ...printers.map(
                    (printer) => Card(
                  child: ListTile(
                    leading: const CircleAvatar(
                      child: Icon(Icons.print),
                    ),
                    title: Text(
                      printer.name.isEmpty
                          ? 'USB Printer'
                          : printer.name,
                    ),
                    subtitle: Text(
                      printer.identifier,
                    ),
                    trailing: connected
                        ? const Icon(
                      Icons.check_circle,
                      color: Colors.green,
                    )
                        : FilledButton(
                      onPressed: loading
                          ? null
                          : () => _connect(printer),
                      child: const Text('CONNECT'),
                    ),
                  ),
                ),
              ),
          ],
        ),
      ),
    );
  }
}
class ProductCatalogScreen extends StatefulWidget {
final String categoryFilter;

const ProductCatalogScreen({
super.key,
this.categoryFilter = 'all',
});

@override
State<ProductCatalogScreen> createState() => _ProductCatalogScreenState();
}

class _ProductCatalogScreenState extends State<ProductCatalogScreen> {
final TextEditingController searchController = TextEditingController();

String searchQuery = '';

List<Product> get visibleProducts {
final q = searchQuery.trim().toLowerCase();

return productCatalog.where((product) {
if (product.deleted) return false;
final categoryMatch = widget.categoryFilter == 'all' || product.category == widget.categoryFilter;
final searchMatch = q.isEmpty || product.name.toLowerCase().contains(q);
return categoryMatch && searchMatch;
}).toList();
}

Future<void> openProductEditor({Product? product, int? index}) async {
final nameController = TextEditingController(text: product?.name ?? '');
final rateController = TextEditingController(
text: product == null || product.rate == 0
? ''
    : product.rate.toStringAsFixed(2),
);

String unit = product?.unit ?? 'piece';
String category =
product?.category ?? (widget.categoryFilter == 'plumbing' ? 'plumbing' : 'general');

final units = [
'kg',
'bag',
'piece',
'ft',
'meter',
'sq ft',
'cft',
'tonne',
'litre',
];

final saved = await showDialog<bool>(
context: context,
builder: (dialogContext) {
return StatefulBuilder(
builder: (context, setDialogState) {
return AlertDialog(
title: Text(
product == null ? 'Add Product' : 'Edit Product',
),
content: SingleChildScrollView(
child: Column(
mainAxisSize: MainAxisSize.min,
children: [
TextField(
controller: nameController,
autofocus: true,
decoration: const InputDecoration(
labelText: 'Product Name',
hintText: 'Example: Cement',
),
),
const SizedBox(height: 12),
DropdownButtonFormField<String>(
initialValue: units.contains(unit) ? unit : 'piece',
decoration: const InputDecoration(
labelText: 'Default Unit',
),
items: units
    .map(
(value) => DropdownMenuItem(
value: value,
child: Text(value),
),
)
    .toList(),
onChanged: (value) {
if (value != null) {
setDialogState(() => unit = value);
}
},
),
const SizedBox(height: 12),
if (widget.categoryFilter == 'all')
DropdownButtonFormField<String>(
initialValue: category,
decoration: const InputDecoration(
labelText: 'Category',
),
items: const [
DropdownMenuItem(
value: 'general',
child: Text('General'),
),
DropdownMenuItem(
value: 'plumbing',
child: Text('Plumbing / Hardware'),
),
],
onChanged: (value) {
if (value != null) {
setDialogState(() => category = value);
}
},
),
const SizedBox(height: 12),
TextField(
controller: rateController,
keyboardType:
const TextInputType.numberWithOptions(decimal: true),
decoration: const InputDecoration(
labelText: 'Default Rate',
hintText: '0',
prefixText: '₹ ',
),
),
],
),
),
actions: [
TextButton(
onPressed: () => Navigator.pop(dialogContext, false),
child: const Text('CANCEL'),
),
FilledButton(
onPressed: () async {
final name = nameController.text.trim();
final rate = double.tryParse(rateController.text.trim()) ?? 0;

if (name.isEmpty || rate < 0) return;

await upsertProduct(
Product(
name: name,
unit: unit,
rate: rate,
category: category,
),
index: index,
);

if (dialogContext.mounted) {
Navigator.pop(dialogContext, true);
}
},
child: const Text('SAVE'),
),
],
);
},
);
},
);

nameController.dispose();
rateController.dispose();

if (saved == true && mounted) {
setState(() {});
}
}

Future<void> deleteProduct(int index) async {
final product = productCatalog[index];

final confirmed = await showDialog<bool>(
context: context,
builder: (dialogContext) {
return AlertDialog(
title: const Text('Delete Product?'),
content: Text(
'"${product.name}" will be removed from quick recommendations.',
),
actions: [
TextButton(
onPressed: () => Navigator.pop(dialogContext, false),
child: const Text('CANCEL'),
),
FilledButton(
onPressed: () => Navigator.pop(dialogContext, true),
style: FilledButton.styleFrom(
backgroundColor: Colors.red,
),
child: const Text('DELETE'),
),
],
);
},
);

if (confirmed == true) {
await deleteProductFromCatalog(index);
if (mounted) setState(() {});
}
}

@override
void initState() {
super.initState();
dataRevisionNotifier.addListener(_onDataChanged);
}

void _onDataChanged() {
if (mounted) setState(() {});
}

@override
Widget build(BuildContext context) {
return Scaffold(
appBar: AppBar(
title: Text(
widget.categoryFilter == 'plumbing' ? 'Plumbing Hardware' : 'Products / Hardware',
style: const TextStyle(fontWeight: FontWeight.bold),
),
actions: [
IconButton(
tooltip: 'Add Product',
onPressed: () => openProductEditor(),
icon: const Icon(Icons.add),
),
],
),
floatingActionButton: FloatingActionButton.extended(
onPressed: () => openProductEditor(),
icon: const Icon(Icons.add),
label: Text(
widget.categoryFilter == 'plumbing' ? 'ADD HARDWARE' : 'ADD PRODUCT',
),
),
body: Column(
children: [
Padding(
padding: const EdgeInsets.fromLTRB(12, 10, 12, 4),
child: TextField(
controller: searchController,
onChanged: (value) {
setState(() {
searchQuery = value;
});
},
decoration: InputDecoration(
hintText: 'Search hardware...',
prefixIcon: const Icon(Icons.search),
suffixIcon: searchQuery.isEmpty
? null
    : IconButton(
icon: const Icon(Icons.clear),
onPressed: () {
searchController.clear();
setState(() {
searchQuery = '';
});
},
),
border: const OutlineInputBorder(),
),
),
),
Expanded(
child: visibleProducts.isEmpty
? Center(
child: Text(
widget.categoryFilter == 'plumbing'
? 'No plumbing hardware added'
    : 'No products added',
),
)
    : ListView.separated(
padding: const EdgeInsets.fromLTRB(12, 12, 12, 90),
itemCount: visibleProducts.length,
separatorBuilder: (_, __) => const SizedBox(height: 6),
itemBuilder: (context, index) {
final product = visibleProducts[index];

return Card(
child: Padding(
padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
child: Row(
children: [
const CircleAvatar(
radius: 22,
child: Icon(Icons.inventory_2_outlined, size: 20),
),
const SizedBox(width: 10),
Expanded(
child: Column(
crossAxisAlignment: CrossAxisAlignment.start,
mainAxisSize: MainAxisSize.min,
children: [
Text(
product.name,
maxLines: 1,
overflow: TextOverflow.ellipsis,
style: const TextStyle(fontWeight: FontWeight.bold),
),
const SizedBox(height: 3),
Text(
'Unit: ${product.unit} • ₹${product.rate.toStringAsFixed(2)}',
maxLines: 1,
overflow: TextOverflow.ellipsis,
style: const TextStyle(fontSize: 12, color: Colors.black54),
),
],
),
),
const SizedBox(width: 4),
IconButton(
constraints: const BoxConstraints.tightFor(width: 40, height: 40),
padding: EdgeInsets.zero,
tooltip: 'Edit Product',
onPressed: () => openProductEditor(product: product, index: productCatalog.indexOf(product)),
icon: const Icon(Icons.edit_outlined, size: 21),
),
IconButton(
constraints: const BoxConstraints.tightFor(width: 40, height: 40),
padding: EdgeInsets.zero,
tooltip: 'Delete Product',
onPressed: () {
final actualIndex = productCatalog.indexOf(product);
if (actualIndex >= 0) deleteProduct(actualIndex);
},
icon: const Icon(Icons.delete_outline, color: Colors.red, size: 21),
),
],
),
),
);
},
),
),
],
),
);
}

@override
void dispose() {
dataRevisionNotifier.removeListener(_onDataChanged);
searchController.dispose();
super.dispose();
}
}

// ============================================================
// BILL HISTORY SCREEN with Search, Edit, Delete, Print, Share
// ============================================================

class BillHistoryScreen extends StatefulWidget {
const BillHistoryScreen({super.key});

@override
State<BillHistoryScreen> createState() => _BillHistoryScreenState();
}

class _BillHistoryScreenState extends State<BillHistoryScreen> {
final TextEditingController searchController = TextEditingController();
String searchQuery = '';

bool _isSameDay(DateTime a, DateTime b) {
return a.year == b.year && a.month == b.month && a.day == b.day;
}

String _dateKey(DateTime date) {
return '${date.year}-${date.month.toString().padLeft(2, '0')}-${date.day.toString().padLeft(2, '0')}';
}

String _formatDate(DateTime date) {
return '${date.day.toString().padLeft(2, '0')}-${date.month.toString().padLeft(2, '0')}-${date.year}';
}

List<Bill> get filteredBills {
final q = searchQuery.trim().toLowerCase();
final result = q.isEmpty
? List<Bill>.from(savedBills)
    : savedBills.where((bill) {
return bill.number.toString().contains(q) ||
bill.customerName.toLowerCase().contains(q) ||
bill.customerMobile.contains(q) ||
bill.items.any((item) => item.name.toLowerCase().contains(q));
}).toList();

result.sort((a, b) {
final dateCompare = b.date.compareTo(a.date);
return dateCompare != 0 ? dateCompare : b.number.compareTo(a.number);
});
return result;
}

Map<String, List<Bill>> get groupedBills {
final groups = <String, List<Bill>>{};
for (final bill in filteredBills) {
final key = _dateKey(bill.date);
groups.putIfAbsent(key, () => []).add(bill);
}
return groups;
}

double _dayTotal(List<Bill> bills) => bills.fold(0, (sum, bill) => sum + bill.total);

@override
void initState() {
super.initState();
dataRevisionNotifier.addListener(_onDataChanged);
refresh();
}

void _onDataChanged() {
if (mounted) setState(() {});
}

Future<void> refresh() async {
await loadBillsFromDatabase();
await runCloudSync();
if (mounted) setState(() {});
}

Future<void> editBill(Bill bill) async {
await Navigator.push(
context,
MaterialPageRoute(builder: (_) => NewBillScreen(editBill: bill)),
);
await loadBillsFromDatabase();
if (mounted) setState(() {});
}

Future<void> deleteBill(Bill bill) async {
final confirmed = await showDialog<bool>(
context: context,
builder: (dialogContext) => AlertDialog(
title: const Text('Delete Bill?'),
content: Text('Bill #${bill.number} will be permanently deleted.'),
actions: [
TextButton(
onPressed: () => Navigator.pop(dialogContext, false),
child: const Text('CANCEL'),
),
FilledButton(
onPressed: () => Navigator.pop(dialogContext, true),
style: FilledButton.styleFrom(backgroundColor: Colors.red),
child: const Text('DELETE'),
),
],
),
);
if (confirmed != true) return;

await AppDatabase.deleteBill(bill.number);
await loadBillsFromDatabase();
if (mounted) {
setState(() {});
ScaffoldMessenger.of(context).showSnackBar(
SnackBar(content: Text('Bill #${bill.number} deleted')),
);
}
}

Widget _buildBillCard(Bill bill) {
return Card(
margin: const EdgeInsets.only(bottom: 8),
child: InkWell(
borderRadius: BorderRadius.circular(12),
onTap: () => editBill(bill),
child: Padding(
padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
child: Row(
children: [
SizedBox(
width: 48,
child: CircleAvatar(
radius: 21,
child: FittedBox(child: Text('${bill.number}')),
),
),
const SizedBox(width: 8),
Expanded(
child: Column(
crossAxisAlignment: CrossAxisAlignment.start,
mainAxisSize: MainAxisSize.min,
children: [
Text(
bill.customerName.isEmpty ? 'Walk-in Customer' : bill.customerName,
maxLines: 1,
overflow: TextOverflow.ellipsis,
style: const TextStyle(fontWeight: FontWeight.w600),
),
const SizedBox(height: 3),
Text(
'${bill.items.length} items • ${bill.customerMobile.isEmpty ? 'No mobile' : bill.customerMobile}',
maxLines: 1,
overflow: TextOverflow.ellipsis,
style: const TextStyle(fontSize: 12, color: Colors.black54),
),
],
),
),
const SizedBox(width: 6),
SizedBox(
width: 82,
child: Text(
'₹${bill.total.toStringAsFixed(2)}',
maxLines: 1,
overflow: TextOverflow.ellipsis,
textAlign: TextAlign.right,
style: const TextStyle(fontWeight: FontWeight.bold),
),
),
IconButton(
constraints: const BoxConstraints.tightFor(width: 40, height: 40),
padding: EdgeInsets.zero,
tooltip: 'Print 58mm',
icon: const Icon(Icons.print, size: 21),
onPressed: () => print80mmBill(context, bill),
),
IconButton(
constraints: const BoxConstraints.tightFor(width: 40, height: 40),
padding: EdgeInsets.zero,
tooltip: 'Share Bill',
icon: const Icon(Icons.share, size: 21),
onPressed: () => showBillShareOptions(context, bill),
),
IconButton(
constraints: const BoxConstraints.tightFor(width: 40, height: 40),
padding: EdgeInsets.zero,
tooltip: 'Delete Bill',
icon: const Icon(Icons.delete_outline, color: Colors.red, size: 21),
onPressed: () => deleteBill(bill),
),
],
),
),
),
);
}

Widget _buildDateSection(String dateKey, List<Bill> bills) {
final date = bills.first.date;
final total = _dayTotal(bills);
final today = _isSameDay(date, DateTime.now());

return Column(
crossAxisAlignment: CrossAxisAlignment.start,
children: [
Container(
width: double.infinity,
margin: const EdgeInsets.only(top: 12),
padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 12),
decoration: BoxDecoration(
color: Colors.white,
borderRadius: BorderRadius.circular(14),
border: Border.all(color: Colors.black12),
),
child: Row(
children: [
Expanded(
child: Column(
crossAxisAlignment: CrossAxisAlignment.start,
children: [
Text(
today ? 'TODAY • ${_formatDate(date)}' : _formatDate(date),
style: const TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
),
const SizedBox(height: 3),
Text(
'${bills.length} bill${bills.length == 1 ? '' : 's'}',
style: const TextStyle(color: Colors.black54, fontSize: 12),
),
],
),
),
Column(
crossAxisAlignment: CrossAxisAlignment.end,
children: [
const Text(
'DAY TOTAL',
style: TextStyle(fontSize: 10, color: Colors.black54, fontWeight: FontWeight.w600),
),
Text(
'₹${total.toStringAsFixed(2)}',
style: const TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
),
],
),
],
),
),
const SizedBox(height: 6),
...bills.map(_buildBillCard),
],
);
}

@override
void dispose() {
dataRevisionNotifier.removeListener(_onDataChanged);
searchController.dispose();
super.dispose();
}

@override
Widget build(BuildContext context) {
final groups = groupedBills;

return Scaffold(
appBar: AppBar(
title: const Text('Bill History', style: TextStyle(fontWeight: FontWeight.bold)),
actions: [
IconButton(tooltip: 'Refresh', onPressed: refresh, icon: const Icon(Icons.refresh)),
],
),
body: Column(
children: [
Padding(
padding: const EdgeInsets.fromLTRB(12, 10, 12, 6),
child: TextField(
controller: searchController,
onChanged: (value) => setState(() => searchQuery = value),
decoration: InputDecoration(
hintText: 'Search bill, customer or product...',
prefixIcon: const Icon(Icons.search),
suffixIcon: searchQuery.isEmpty
? null
    : IconButton(
icon: const Icon(Icons.clear),
onPressed: () {
searchController.clear();
setState(() => searchQuery = '');
},
),
border: const OutlineInputBorder(),
),
),
),
Expanded(
child: savedBills.isEmpty
? const Center(child: Text('No bills found', style: TextStyle(fontSize: 17, color: Colors.grey)))
    : groups.isEmpty
? const Center(child: Text('No matching bills', style: TextStyle(fontSize: 17, color: Colors.grey)))
    : ListView(
padding: const EdgeInsets.fromLTRB(12, 0, 12, 20),
children: groups.entries
    .map((entry) => _buildDateSection(entry.key, entry.value))
    .toList(),
),
),
],
),
);
}
}
class ExpenseScreen extends StatefulWidget {
  const ExpenseScreen({super.key});

  @override
  State<ExpenseScreen> createState() => _ExpenseScreenState();
}

class _ExpenseScreenState extends State<ExpenseScreen> {
  final titleController = TextEditingController();
  final amountController = TextEditingController();
  String category = 'General';

  final categories = const [
    'General',
    'Transport',
    'Labour',
    'Electricity',
    'Shop',
    'Other',
  ];

  bool _isSameDay(DateTime a, DateTime b) {
    return a.year == b.year &&
        a.month == b.month &&
        a.day == b.day;
  }

  String _formatDate(DateTime date) {
    return '${date.day.toString().padLeft(2, '0')}-'
        '${date.month.toString().padLeft(2, '0')}-'
        '${date.year}';
  }

  double get todayTotal {
    final now = DateTime.now();

    return savedExpenses
        .where((e) => _isSameDay(e.date, now))
        .fold(0.0, (sum, e) => sum + e.amount);
  }

  Future<void> addExpense() async {
    final title = titleController.text.trim();
    final amount = double.tryParse(
      amountController.text.trim(),
    );

    if (title.isEmpty || amount == null || amount <= 0) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('Expense name aur valid amount enter karo'),
        ),
      );
      return;
    }

    final now = DateTime.now();

    final expense = Expense(
      id: _newSyncId('expense'),
      title: title,
      category: category,
      amount: amount,
      date: now,
      createdAt: now,
      updatedAt: now,
    );

    await CloudSync.saveExpense(expense);

    titleController.clear();
    amountController.clear();

    if (mounted) {
      setState(() {});

      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('Expense saved successfully'),
        ),
      );
    }
  }

  Future<void> deleteExpense(Expense expense) async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Delete Expense?'),
        content: Text(
          '${expense.title} - ₹${expense.amount.toStringAsFixed(2)}',
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(context, false),
            child: const Text('CANCEL'),
          ),
          FilledButton(
            onPressed: () => Navigator.pop(context, true),
            style: FilledButton.styleFrom(
              backgroundColor: Colors.red,
            ),
            child: const Text('DELETE'),
          ),
        ],
      ),
    );

    if (confirmed != true) return;

    savedExpenses.removeWhere(
          (e) => e.id == expense.id,
    );

    notifyDataChanged();

    if (mounted) {
      setState(() {});
    }
  }

  @override
  void initState() {
    super.initState();

    dataRevisionNotifier.addListener(_refresh);
  }

  void _refresh() {
    if (mounted) {
      setState(() {});
    }
  }

  @override
  void dispose() {
    dataRevisionNotifier.removeListener(_refresh);
    titleController.dispose();
    amountController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final now = DateTime.now();

    final todayExpenses = savedExpenses
        .where((e) => _isSameDay(e.date, now))
        .toList()
      ..sort((a, b) => b.date.compareTo(a.date));

    return Scaffold(
      appBar: AppBar(
        title: const Text(
          'Today Expenses',
          style: TextStyle(fontWeight: FontWeight.bold),
        ),
      ),
      body: Column(
        children: [
          Container(
            width: double.infinity,
            margin: const EdgeInsets.all(16),
            padding: const EdgeInsets.all(20),
            decoration: BoxDecoration(
              color: const Color(0xff253f3a),
              borderRadius: BorderRadius.circular(18),
            ),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  "TODAY'S EXPENSES",
                  style: TextStyle(
                    color: Colors.white70,
                    fontWeight: FontWeight.w600,
                  ),
                ),
                const SizedBox(height: 8),
                Text(
                  '₹${todayTotal.toStringAsFixed(2)}',
                  style: const TextStyle(
                    color: Colors.white,
                    fontSize: 30,
                    fontWeight: FontWeight.bold,
                  ),
                ),
                const SizedBox(height: 4),
                Text(
                  _formatDate(now),
                  style: const TextStyle(
                    color: Colors.white70,
                  ),
                ),
              ],
            ),
          ),

          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 16),
            child: Card(
              child: Padding(
                padding: const EdgeInsets.all(16),
                child: Column(
                  children: [
                    TextField(
                      controller: titleController,
                      decoration: const InputDecoration(
                        labelText: 'Expense Name',
                        prefixIcon: Icon(Icons.receipt_long),
                        border: OutlineInputBorder(),
                      ),
                    ),
                    const SizedBox(height: 10),

                    DropdownButtonFormField<String>(
                      initialValue: category,
                      decoration: const InputDecoration(
                        labelText: 'Category',
                        border: OutlineInputBorder(),
                      ),
                      items: categories
                          .map(
                            (c) => DropdownMenuItem(
                          value: c,
                          child: Text(c),
                        ),
                      )
                          .toList(),
                      onChanged: (value) {
                        if (value != null) {
                          setState(() => category = value);
                        }
                      },
                    ),

                    const SizedBox(height: 10),

                    TextField(
                      controller: amountController,
                      keyboardType:
                      const TextInputType.numberWithOptions(
                        decimal: true,
                      ),
                      decoration: const InputDecoration(
                        labelText: 'Amount',
                        prefixText: '₹ ',
                        border: OutlineInputBorder(),
                      ),
                    ),

                    const SizedBox(height: 12),

                    SizedBox(
                      width: double.infinity,
                      height: 52,
                      child: FilledButton.icon(
                        onPressed: addExpense,
                        icon: const Icon(Icons.add),
                        label: const Text(
                          'ADD EXPENSE',
                          style: TextStyle(
                            fontWeight: FontWeight.bold,
                          ),
                        ),
                      ),
                    ),
                  ],
                ),
              ),
            ),
          ),

          const SizedBox(height: 10),

          Expanded(
            child: todayExpenses.isEmpty
                ? const Center(
              child: Text(
                'No expenses today',
                style: TextStyle(
                  color: Colors.grey,
                  fontSize: 17,
                ),
              ),
            )
                : ListView.builder(
              padding: const EdgeInsets.fromLTRB(
                16,
                0,
                16,
                20,
              ),
              itemCount: todayExpenses.length,
              itemBuilder: (context, index) {
                final expense = todayExpenses[index];

                return Card(
                  child: ListTile(
                    leading: const CircleAvatar(
                      child: Icon(Icons.money_off),
                    ),
                    title: Text(
                      expense.title,
                      style: const TextStyle(
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    subtitle: Text(
                      '${expense.category} • '
                          '${_formatDate(expense.date)}',
                    ),
                    trailing: Row(
                      mainAxisSize: MainAxisSize.min,
                      children: [
                        Text(
                          '₹${expense.amount.toStringAsFixed(2)}',
                          style: const TextStyle(
                            fontWeight: FontWeight.bold,
                          ),
                        ),
                        IconButton(
                          icon: const Icon(
                            Icons.delete_outline,
                            color: Colors.red,
                          ),
                          onPressed: () =>
                              deleteExpense(expense),
                        ),
                      ],
                    ),
                  ),
                );
              },
            ),
          ),
        ],
      ),
    );
  }
}
