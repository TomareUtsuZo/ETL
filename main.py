import duckdb
from extracts import query_all_records
from load import load_data_to_duckdb
from transform import transform_data_in_duckdb


def main():
    print("Hello world!")
    all_records = query_all_records(10,2) # batch_size=10, number_of_batches_to_process=2)

    print("Extracted DataFrame:\n", all_records)

    conn = duckdb.connect("pokemon_data.db")
    load_data_to_duckdb(all_records, conn)
    transform_data_in_duckdb(conn)
    conn.close()

if __name__ == "__main__":
    main()


