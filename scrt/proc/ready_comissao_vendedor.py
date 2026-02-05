import os
import json
import time
import logging
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
import gspread

# Configure logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

class ComissaoProcessingError(Exception):
    """Custom exception for Comissão processing failures"""
    pass

class GoogleSheetsError(Exception):
    """Custom exception for Google Sheets operations"""
    pass

class ComissaoProcessor:
    def __init__(self, directory: str = "."):
        self.directory = directory
        logging.info(f"Initialized Comissão Processor with directory: {directory}")
    
    def get_latest_file(self, extension: str = 'xls'):
        """Get the most recently modified file with a given extension"""
        try:
            import glob
            search_pattern = os.path.join(self.directory, f'*.{extension}')
            files = glob.glob(search_pattern)
            
            if not files:
                raise ComissaoProcessingError(f"No .{extension} files found in {self.directory}")
            
            latest_file = max(files, key=os.path.getmtime)
            logging.info(f"Found latest file: {os.path.basename(latest_file)}")
            return latest_file
            
        except Exception as e:
            raise ComissaoProcessingError(f"Failed to find latest file: {str(e)}")
    
    def retry_api_call(self, func, retries=3, delay=2):
        """Retry API calls on 500 errors"""
        for i in range(retries):
            try:
                return func()
            except HttpError as error:
                if hasattr(error, "resp") and error.resp.status == 500:
                    logging.warning(f"APIError 500 encountered. Retrying {i + 1}/{retries}...")
                    time.sleep(delay)
                else:
                    raise
        raise GoogleSheetsError("Max retries reached for API call")
    
    def process_excel_data(self, input_file: str):
        """Process commission Excel file with header row 10"""
        if not os.path.exists(input_file):
            raise ComissaoProcessingError(f"File does not exist: {input_file}")
        
        if os.path.getsize(input_file) == 0:
            raise ComissaoProcessingError(f"File is empty: {input_file}")
        
        try:
            logging.info(f"Processing commission Excel file: {os.path.basename(input_file)}")
            
            # Determine engine based on file extension
            engine = "xlrd" if input_file.lower().endswith(".xls") else None
            
            # Read Excel starting from row 10 (0-indexed)
            df = pd.read_excel(input_file, header=10, engine=engine)
            
            if df.empty:
                raise ComissaoProcessingError("Excel file contains no data after skipping rows")
            
            # Define required columns
            required_cols = [
                "Código",
                "Vendedor", 
                "Base Comissão",
                "% Comissão",
                "Valor Comissão"
            ]
            
            # Check for missing columns
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise ComissaoProcessingError(f"Missing required columns: {missing_cols}")
            
            # Process rows to extract filial information
            filial = None
            resultados = []
            
            for _, row in df.iterrows():
                codigo = str(row["Código"])
                
                # Check if this row indicates a new filial
                if "Filial:" in codigo:
                    filial = str(row["Vendedor"]).strip()
                    logging.info(f"Found Filial: {filial}")
                
                # Process employee rows (numeric código)
                elif codigo.isnumeric():
                    if not filial:
                        logging.warning(f"Código {codigo} without Filial. Skipping.")
                        continue
                    
                    resultados.append({
                        "Código": codigo,
                        "Colaborador": row["Vendedor"],
                        "Filial": filial,
                        "Base Comissão": row["Base Comissão"],
                        "% Comissão": row["% Comissão"],
                        "Valor Comissão": row["Valor Comissão"]
                    })
            
            # Create result DataFrame
            result_df = pd.DataFrame(resultados)
            
            if result_df.empty:
                raise ComissaoProcessingError("No valid employee rows found after processing")
            
            # Format Filial column (2-digit with leading zeros)
            result_df["Filial"] = result_df["Filial"].astype(int).astype(str).str.zfill(2)
            
            # Reorder columns
            result_df = result_df[[
                "Filial", 
                "Código", 
                "Colaborador", 
                "Base Comissão", 
                "% Comissão", 
                "Valor Comissão"
            ]]
            
            logging.info(f"✓ Processing complete. Rows processed: {len(result_df)}")
            return result_df
            
        except pd.errors.EmptyDataError:
            raise ComissaoProcessingError("Excel file is empty or corrupted")
        except Exception as e:
            raise ComissaoProcessingError(f"Failed to process Excel file: {str(e)}")


class ComissaoSheetsUploader:
    def __init__(self, credentials_json: str, sheet_id: str):
        if not credentials_json:
            raise GoogleSheetsError("Google credentials JSON cannot be empty")
        if not sheet_id:
            raise GoogleSheetsError("Sheet ID cannot be empty")
        
        self.credentials_json = credentials_json
        self.sheet_id = sheet_id
        self.client = None
        
    def authenticate(self):
        """Authenticate with Google Sheets API"""
        try:
            logging.info("Authenticating with Google Sheets API...")
            creds_dict = json.loads(self.credentials_json)
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            self.client = gspread.authorize(creds)
            logging.info("✓ Authentication successful")
            
        except json.JSONDecodeError as e:
            raise GoogleSheetsError(f"Invalid JSON in credentials: {str(e)}")
        except Exception as e:
            raise GoogleSheetsError(f"Authentication failed: {str(e)}")
    
    def upload_comissoes(self, df: pd.DataFrame, worksheet_name: str = "COMISSOES"):
        """Upload comissões data to Google Sheets"""
        if not self.client:
            raise GoogleSheetsError("Client not authenticated. Call authenticate() first")
        
        if df.empty:
            raise GoogleSheetsError("DataFrame is empty. Nothing to upload")
        
        try:
            # Open spreadsheet and worksheet
            logging.info(f"Opening spreadsheet with ID: {self.sheet_id}")
            spreadsheet = self.client.open_by_key(self.sheet_id)
            sheet = spreadsheet.worksheet(worksheet_name)
            logging.info(f"✓ Accessed worksheet: {worksheet_name}")
            
            # Prepare data
            logging.info("Preparing data for Google Sheets...")
            df = df.fillna("")  # Ensure no NaN values
            rows = [df.columns.tolist()] + df.values.tolist()
            
            # Clear sheet and update
            logging.info("Clearing existing data...")
            sheet.clear()
            
            logging.info(f"Uploading {len(df)} rows of data...")
            # Use retry mechanism for API call
            for i in range(3):
                try:
                    sheet.update(rows)
                    break
                except HttpError as error:
                    if hasattr(error, "resp") and error.resp.status == 500 and i < 2:
                        logging.warning(f"APIError 500 encountered. Retrying {i + 1}/3...")
                        time.sleep(2)
                    else:
                        raise
            
            logging.info("✓ Comissões data uploaded successfully to Google Sheets")
            
        except gspread.exceptions.APIError as e:
            raise GoogleSheetsError(f"Google Sheets API error: {str(e)}")
        except Exception as e:
            raise GoogleSheetsError(f"Failed to upload comissões data: {str(e)}")


def main():
    """Main execution with proper error handling for GitHub Actions"""
    try:
        # Get environment variables
        download_dir = os.getenv("DOWNLOAD_DIR", "/home/runner/work/comp_meta/comp_meta/")
        sheet_id = os.getenv("SOURCE_SHEET_ID")
        gsa_credentials = os.getenv("GSA_CREDENTIALS")
        
        # Validate environment variables
        if not sheet_id:
            raise GoogleSheetsError("SHEET_ID environment variable not set")
        if not gsa_credentials:
            raise GoogleSheetsError("GSA_CREDENTIALS environment variable not set")
        
        logging.info("Environment variables loaded successfully")
        logging.info(f"Download directory: {download_dir}")
        logging.info(f"Directory exists: {os.path.exists(download_dir)}")
        
        # Wait for potential file downloads to complete
        logging.info("Waiting for potential file downloads to complete...")
        time.sleep(15)
        
        # Process Excel file
        processor = ComissaoProcessor(directory=download_dir)
        latest_file = processor.get_latest_file(extension='xls')
        processed_df = processor.process_excel_data(latest_file)
        
        # Show preview
        logging.info("\n=== COMISSÕES DATA PREVIEW ===")
        logging.info(f"Total rows: {len(processed_df)}")
        logging.info(f"Columns: {list(processed_df.columns)}")
        if not processed_df.empty:
            logging.info("\nFirst 5 rows:")
            logging.info(processed_df.head().to_string())
        
        # Upload to Google Sheets
        uploader = ComissaoSheetsUploader(gsa_credentials, sheet_id)
        uploader.authenticate()
        uploader.upload_comissoes(processed_df, "COMISSOES")
        
        # Clean up: remove the processed file
        logging.info(f"Removing processed file: {os.path.basename(latest_file)}")
        os.remove(latest_file)
        
        logging.info("✓ Process completed successfully")
        return 0  # Success exit code
        
    except (ComissaoProcessingError, GoogleSheetsError) as e:
        logging.error(f"✗ {e.__class__.__name__}: {str(e)}")
        return 1  # Business logic failure
    except Exception as e:
        logging.error(f"✗ Unexpected error: {str(e)}")
        return 2  # Unexpected failure


if __name__ == "__main__":
    # Exit with proper code for GitHub Actions
    exit_code = main()
    exit(exit_code)
