"""
Test database connection and basic queries
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db_connection import get_db
from utils.logger import get_logger

logger = get_logger("TestConnection")


def test_connection():
    """Test basic database connectivity"""
    print("Testing database connection...")
    db = get_db()
    
    success, error = db.test_connection()
    
    if success:
        print("✓ Connection successful!")
        return True
    else:
        print(f"✗ Connection failed: {error}")
        return False


def test_get_tables():
    """Test retrieving table names"""
    print("\nTesting table retrieval...")
    db = get_db()
    
    tables, error = db.get_table_names()
    
    if error:
        print(f"✗ Failed to get tables: {error}")
        return False
    
    print(f"✓ Found {len(tables)} tables:")
    for table in tables:
        print(f"  - {table}")
    
    return True


def test_get_schema():
    """Test retrieving table schema"""
    print("\nTesting schema retrieval...")
    db = get_db()
    
    # Get first table
    tables, _ = db.get_table_names()
    if not tables:
        print("✗ No tables found")
        return False
    
    test_table = tables[0]
    print(f"Getting schema for table: {test_table}")
    
    schema, error = db.get_table_schema(test_table)
    
    if error:
        print(f"✗ Failed to get schema: {error}")
        return False
    
    print(f"✓ Found {len(schema)} columns:")
    for col in schema[:5]:  # Show first 5
        print(f"  - {col['column_name']} ({col['data_type']})")
    
    return True


def test_simple_query():
    """Test executing a simple query"""
    print("\nTesting simple query execution...")
    db = get_db()
    
    query = "SELECT * FROM products LIMIT 3"
    
    results, error = db.execute_query(query)
    
    if error:
        print(f"✗ Query failed: {error}")
        return False
    
    print(f"✓ Query successful! Retrieved {len(results)} rows:")
    for row in results:
        print(f"  - {dict(row)}")
    
    return True


def test_relationships():
    """Test retrieving table relationships"""
    print("\nTesting relationship retrieval...")
    db = get_db()
    
    relationships, error = db.get_table_relationships()
    
    if error:
        print(f"✗ Failed to get relationships: {error}")
        return False
    
    print(f"✓ Found {len(relationships)} relationships:")
    for rel in relationships[:5]:  # Show first 5
        print(f"  - {rel['from_table']}.{rel['from_column']} -> {rel['to_table']}.{rel['to_column']}")
    
    return True


def main():
    """Run all tests"""
    print("="*60)
    print("Database Connection Tests")
    print("="*60)
    
    tests = [
        ("Connection Test", test_connection),
        ("Table Retrieval", test_get_tables),
        ("Schema Retrieval", test_get_schema),
        ("Simple Query", test_simple_query),
        ("Relationships", test_relationships)
    ]
    
    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
        except Exception as e:
            print(f"✗ Test failed with exception: {str(e)}")
            results.append((name, False))
        print()
    
    print("="*60)
    print("Test Results Summary")
    print("="*60)
    
    for name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {name}")
    
    total_passed = sum(1 for _, success in results if success)
    print(f"\nPassed: {total_passed}/{len(results)}")
    
    return total_passed == len(results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)