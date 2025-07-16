from config import get_collection

def clear_collection():
    """Clear the speakers collection"""
    collection = get_collection()
    result = collection.delete_many({})
    print(f"Deleted {result.deleted_count} documents from the collection")

if __name__ == "__main__":
    response = input("Are you sure you want to clear the speakers collection? (yes/no): ")
    if response.lower() == 'yes':
        clear_collection()
    else:
        print("Cancelled")