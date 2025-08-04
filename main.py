import os
import lz4.frame
from typing import TYPE_CHECKING
from botocore.exceptions import ClientError
from boto3.session import Session
from datetime import datetime, timedelta

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


def get_session(region_name: str | None = None) -> Session:
    return Session(region_name=region_name) if region_name else Session()


def get_s3_client(session: Session) -> "S3Client":
    return session.client("s3")


def backward_fetching(
    coin: str,
    start_date: str,
    end_date: str,
    output_path: str,
) -> None:
    """
    Fetches historical market data from S3 bucket backward from end_date to start_date.
    Implements retry logic for missing hourly and daily data.

    Args:
        coin: Cryptocurrency symbol (e.g., 'SOL')
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        output_path: Local directory to save downloaded files
    """
    session = get_session()
    s3_client = get_s3_client(session)

    current_date = datetime.strptime(end_date, "%Y%m%d")
    start_date_dt = datetime.strptime(start_date, "%Y%m%d")

    bucket_name = "hyperliquid-archive"
    max_hourly_retries = 3
    max_daily_retries = 3
    daily_retry_count = 0

    os.makedirs(output_path, exist_ok=True)

    while current_date >= start_date_dt and daily_retry_count < max_daily_retries:
        hourly_retry_count = 0
        current_hour = 23  # Start from the last hour of the day

        while current_hour >= 0 and daily_retry_count < max_daily_retries:
            date_str = current_date.strftime("%Y%m%d")
            hour_str = f"{current_hour}"
            s3_key = f"market_data/{date_str}/{hour_str}/l2Book/{coin}.lz4"
            local_file = os.path.join(output_path, f"{date_str}_{hour_str}_{coin}.txt")

            try:
                response = s3_client.get_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    RequestPayer="requester",
                )
                streaming_body = response["Body"]
                with open(local_file, "wb") as local_f:
                    decompressor = lz4.frame.LZ4FrameDecompressor()
                    for chunk in streaming_body.iter_chunks(chunk_size=4096):
                        local_f.write(decompressor.decompress(chunk))

                    local_f.write(decompressor.decompress(b""))
                    local_f.flush()
                    os.fsync(local_f.fileno())

                print(f"Successfully downloaded: {s3_key}")

                hourly_retry_count = 0  # Reset hourly retry count on success
                current_hour -= 1  # Move to previous hour

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "NoSuchKey":
                    print(f"File not found: {s3_key}")
                    hourly_retry_count += 1

                    if hourly_retry_count >= max_hourly_retries:
                        print(
                            f"Exhausted {max_hourly_retries} hourly retries for {date_str}"
                        )
                        current_date -= timedelta(days=1)
                        current_hour = 0
                        hourly_retry_count = 0
                        daily_retry_count += 1
                        print(
                            f"Moving to previous day: {current_date.strftime('%Y%m%d')}, Daily retries left: {max_daily_retries - daily_retry_count}"
                        )
                        continue
                    else:
                        current_hour -= 2
                        if current_hour < 0:
                            current_date -= timedelta(days=1)
                            current_hour = 0
                            daily_retry_count += 1
                            print(
                                f"Moving to previous day due to hour underflow: {current_date.strftime('%Y%m%d')}, Daily retries left: {max_daily_retries - daily_retry_count}"
                            )
                            continue

                else:
                    print(f"Error downloading {s3_key}: {str(e)}")
                    raise e

            except Exception as e:
                print(f"Unexpected error downloading {s3_key}: {str(e)}")
                raise e

        if current_hour < 0 and hourly_retry_count == 0:
            current_date -= timedelta(days=1)
            current_hour = 23

    if daily_retry_count >= max_daily_retries:
        print(
            f"Exhausted {max_daily_retries} daily retries. No more historical data available."
        )
    else:
        print("Completed fetching all available historical data.")


if __name__ == "__main__":
    backward_fetching(
        "BTC",
        "20250601",
        "20250630",
        "/Volumes/Samsung_T7/Hyperliquid/BTCUSDT",
    )
