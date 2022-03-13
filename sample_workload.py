import csv

TOTAL_COLUMN_COUNT = 26
STATEMENT_COLUMN = 13
CLIENT_COLUMN = 23

def sample_workload(workload_csv, sample_count):
    with open(workload_csv, 'r') as fr:
        reader = csv.reader(fr)

        is_in_txn = False
        collected_queries = []

        for row in reader:
            if len(collected_queries) >= sample_count:
                break

            if len(row) != TOTAL_COLUMN_COUNT or row[CLIENT_COLUMN] != 'client backend':
                continue

            if not row[STATEMENT_COLUMN].startswith('statement: '):
                continue

            statement = row[STATEMENT_COLUMN][len('statement: '):]
            if statement == 'BEGIN':
                is_in_txn = True
            elif statement == 'COMMIT':
                is_in_txn = False
            elif is_in_txn and not statement.startswith('SET'):
                collected_queries.append(statement)

    with open('sample_workload', 'w') as fw:
        for q in collected_queries:
            fw.write(f"{q}\n")
        
    return collected_queries