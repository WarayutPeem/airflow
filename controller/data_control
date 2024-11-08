from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


def to_decimal(value):
    try:
        # Convert value to Decimal
        return Decimal(value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError, ValueError):
        # Handle invalid values
        return None
    
    
def is_valid_date_format(date_string):
    # Regular expression pattern for YYYY-MM-DD format
    pattern_date = r"^\d{4}-\d{2}-\d{2}$"
    pattern_datetime = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"
    
    # Match the pattern with the input string
    if re.match(pattern_date, date_string) or re.match(pattern_datetime, date_string):
        return True
    else:
        return False
    
    
def format_column_date(date_string):
    if date_string != None and is_valid_date_format(date_string):
        return date_string[:10]
    
    
def format_column_datetime(date_string):
    if date_string != None and is_valid_date_format(date_string):
        if len(date_string) == 19:
            return date_string
        else:
            return date_string[:10] + ' 00:00:00'


def clean_fix_type_data(df, schema, field_action, key):
    field_type_date = field_action["field_type_date"]
    field_encrypt = field_action["field_encrypt"]
    
    for field in schema["fields"]:
        field_name = field["name"]
        field_type = field["type"]

        if isinstance(field_type, list) and "null" in field_type:
            primary_type = next(t for t in field_type if t != "null")
        else:
            primary_type = field_type

        if primary_type == "long":
            df[field_name] = df[field_name].astype("Int64")

        elif primary_type == "int":
            df[field_name] = df[field_name].astype("Int64")
        
        elif primary_type == "float":
            df[field_name] = df[field_name].astype(float)

        elif primary_type == "double":
            df[field_name] = df[field_name].astype(float)
        
        elif primary_type == "boolean":
            df[field_name] = df[field_name].astype("boolean")

        elif primary_type == "string":
            df[field_name] = df[field_name].astype(str).replace("None", None)

        elif isinstance(primary_type, dict) and primary_type.get("logicalType") == "decimal":
            df[field_name] = df[field_name].apply(to_decimal)

        # convert to date
        elif primary_type == "string" and field_name in field_type_date:
            df[field_name] = (
                df[field_name]
                .astype(str)
                .replace("NaT", None)
                .replace("None", None)
                .apply(format_column_date)
            )

        # not found type
        else:
            df[field_name] = df[field_name].astype(str).replace("None", None)
            
    # encrypt data
    df = encrypt_cols_in_df(df, field_encrypt, key)
    
    return df