

def preprocess_transaction(transactions):
    
    # latest_proc = Verify_Completed_Data_Function()
    # customer_dict, terminal_dict = get_dictionaries(latest_proc, customer_columns, terminal_columns)

    transactions = {k:[v] for k,v in transactions.items()}
    # transactions = pd.DataFrame(transactions)
    print(transactions)

preprocess_transaction(transactions='df')