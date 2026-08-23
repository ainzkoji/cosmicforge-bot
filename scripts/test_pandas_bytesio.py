import pandas as pd
import io
import sys

try:
    df = pd.DataFrame([{"col1": 1, "col2": "test"}])
    print("Testing BytesIO...")
    stream_bytes = io.BytesIO()
    try:
        df.to_csv(stream_bytes, index=False)
        print("BytesIO Success")
    except Exception as e:
        print(f"BytesIO Failed: {e}")

    print("Testing StringIO...")
    stream_string = io.StringIO()
    try:
        df.to_csv(stream_string, index=False)
        print("StringIO Success")
    except Exception as e:
        print(f"StringIO Failed: {e}")

    print("Testing TextIOWrapper...")
    stream_bytes_wrapped = io.BytesIO()
    wrapper = io.TextIOWrapper(stream_bytes_wrapped, encoding='utf-8', write_through=True)
    try:
        df.to_csv(wrapper, index=False)
        # wrapper.detach() # Don't close bytes
        print("TextIOWrapper Success")
        print(f"Content: {stream_bytes_wrapped.getvalue()}")
    except Exception as e:
        print(f"TextIOWrapper Failed: {e}")

except Exception as e:
    print(f"General Failure: {e}")
