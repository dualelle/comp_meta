import os
import json
import time
import logging
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
import gspread
import xlrd
from openpyxl import Workbook

# Configure logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

class VendasVendedorError(Exception):
    """Custom exception for Vendas Vendedor processing failures"""
    pass

class GoogleSheetsError(Exception):
    """Custom exception for Google Sheets operations"""
    pass

class VendasVendedorProcessor:
    def __init__(self, directory: str = "."):
        self.directory = directory
        logging.info(f"Initialized Vendas Vendedor Processor with directory: {directory}")
    
    def get_latest_file(self):
        """Get the most recently modified Excel file (.xls or .xlsx)"""
        try:
            import glob
            # Search for both .xls and .xlsx files
            xls_files = glob.glob(os.path.join(self.directory, '*.xls'))
            xlsx_files = glob.glob(os.path.join(self.directory, '*.xlsx'))
            all_files = xls_files + xlsx_files
            
            if not all_files:
                raise VendasVendedorError(f"No Excel files found in {self.directory}")
            
            latest_file = max(all_files, key=os.path.getmtime)
            logging.info(f"Found latest file: {os.path.basename(latest_file)}")
            return latest_file
            
        except Exception as e:
            raise VendasVendedorError(f"Failed to find latest file: {str(e)}")
    
    def convert_xls_to_xlsx(self, file_path: str) -> str:
        """Convert .xls file to .xlsx format if needed"""
        if file_path.lower().endswith(".xlsx"):
            logging.info("File already .xlsx, skipping conversion.")
            return file_path
        
        logging.info("Converting .xls to .xlsx...")
        
        try:
            # Try to open as real .xls
            book = xlrd.open_workbook(file_path)
            sheet = book.sheet_by_index(0)
            
            # Create new .xlsx workbook
            wb = Workbook()
            ws = wb.active
            
            # Copy all rows
            for row_idx in range(sheet.nrows):
                ws.append(sheet.row_values(row_idx))
            
            # Save as .xlsx
            new_path = file_path.replace(".xls", ".xlsx")
            wb.save(new_path)
            
            logging.info(f"✓ Converted to: {os.path.basename(new_path)}")
            return new_path
            
        except xlrd.biffh.XLRDError:
            # File is not a real .xls (might be .xls extension but Excel 2007+ format)
            logging.warning("File is not a real .xls. Renaming to .xlsx.")
            new_path = file_path.replace(".xls", ".xlsx")
            os.rename(file_path, new_path)
            return new_path
        except Exception as e:
            raise VendasVendedorError(f"Failed to convert file: {str(e)}")
    
    @staticmethod
    def format_qtd_vendas(value):
        """Format quantity values with proper decimal formatting"""
        try:
            if pd.isna(value):
                return ""
            
            value = float(value)
            if value.is_integer():
                return f"{int(value):,}".replace(",", ".")
            return f"{value:,}".replace(",", ".")
        except (ValueError, TypeError):
            return str(value) if value else ""
    
    def process_excel_data(self, file_path: str):
        """Process Vendas Vendedor Excel file with header row 9"""
        if not os.path.exists(file_path):
            raise VendasVendedorError(f"File does not exist: {file_path}")
        
        if os.path.getsize(file_path) == 0:
            raise VendasVendedorError(f"File is empty: {file_path}")
        
        try:
            logging.info(f"Processing Vendas Vendedor Excel file: {os.path.basename(file_path)}")
            
            # Read Excel starting from row 9 (0-indexed, so header=9 means row 10)
            df = pd.read_excel(
                file_path,
                header=9,  # Start from row 10
                dtype={"qtd. vendas": str}
            )
            
            if df.empty:
                raise VendasVendedorError("Excel file contains no data after skipping rows")
            
            # Normalize column names
            df.columns = df.columns.str.strip().str.lower()
            
            # Log columns for debugging
            logging.info(f"Columns found: {list(df.columns)}")
            
            # Process rows
            current_filial = None
            data = []
            
            for idx, row in df.iterrows():
                codigo_raw = str(row.get("código", "")).strip()
                
                # Check if this row indicates a new filial
                if "filial:" in codigo_raw.lower():
                    current_filial = row.get("unnamed: 3")
                    if pd.notna(current_filial):
                        current_filial = str(current_filial).strip()
                        logging.info(f"Found Filial: {current_filial}")
                    else:
                        current_filial = None
                    continue
                
                # Process employee rows (numeric código)
                if codigo_raw.isdigit():
                    if not current_filial:
                        logging.warning(f"Código {codigo_raw} without Filial. Skipping.")
                        continue
                    
                    # Format Filial as 2-digit
                    try:
                        filial_formatted = f"{int(float(current_filial)):02d}"
                    except (ValueError, TypeError):
                        filial_formatted = str(current_filial)
                    
                    # Extract and format data
                    vendedor = row.get("vendedor", "")
                    qtd_vendas = self.format_qtd_vendas(row.get("qtd. vendas"))
                    valor_custo = row.get("valor custo")
                    valor_vendas = row.get("valor vendas")
                    
                    data.append({
                        "Código": codigo_raw,
                        "Filial": filial_formatted,
                        "Colaborador": vendedor if pd.notna(vendedor) else "",
                        "Qtd Vendas": qtd_vendas,
                        "Coluna Vazia": "",  # Empty column as per requirements
                        "Valor Custo": valor_custo if pd.notna(valor_custo) else "",
                        "Faturamento": valor_vendas if pd.notna(valor_vendas) else "",
                    })
            
            # Create result DataFrame
            result_df = pd.DataFrame(data)
            
            if result_df.empty:
                raise VendasVendedorError("No valid vendedor rows found after processing")
            
            logging.info(f"✓ Processing complete. Rows processed: {len(result_df)}")
            return result_df
            
        except pd.errors.EmptyDataError:
            raise VendasVendedorError("Excel file is empty or corrupted")
        except Exception as e:
            raise VendasVendedorError(f"Failed to process Excel file: {str(e)}")


class VendasVendedorSheetsUploader:
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
    
    def upload_vendas_vendedor(self, df: pd.DataFrame, worksheet_name: str = "VENDAS_VENDEDOR"):
        """Upload Vendas Vendedor data to Google Sheets"""
        if not self.client:
            raise GoogleSheetsError("Client not authenticated. Call authenticate() first")
        
        if df.empty:
            raise GoogleSheetsError("DataFrame is empty. Nothing to upload")
        
        try:
            # Open spreadsheet and worksheet
            logging.info(f"Opening spreadsheet with ID: {self.sheet_id}")
            spreadsheet = self.client.open_by_key(self.sheet_id)
            worksheet = spreadsheet.worksheet(worksheet_name)
            logging.info(f"✓ Accessed worksheet: {worksheet_name}")
            
            # Prepare data - rename columns as per specification
            logging.info("Preparing data for Google Sheets...")
            
            # Rename columns as specified
            df = df.rename(columns={
                "Faturamento": "Valor Vendas",
                "Colaborador": "Vendedor",
            })
            
            # Define column order as per specification
            COLUMN_ORDER = [
                "Filial",
                "Código",
                "Vendedor",
                "Valor Vendas",
            ]
            
            # Keep only specified columns
            df = df[COLUMN_ORDER]
            
            # Fill NaN with empty strings
            df = df.fillna("")
            
            # Clear existing data
            logging.info("Clearing existing data...")
            worksheet.batch_clear(["A1:Z"])
            
            # Prepare values for upload
            values = [df.columns.tolist()] + df.values.tolist()
            
            # Calculate dynamic range
            start_cell = "A1"
            end_row = len(df) + 1  # +1 for header
            end_column = chr(ord('A') + len(COLUMN_ORDER) - 1)  # Last column letter
            dynamic_range = f"{start_cell}:{end_column}{end_row}"
            
            logging.info(f"Uploading {len(df)} rows to range {dynamic_range}...")
            
            # Update sheet with retry mechanism
            update_func = lambda: worksheet.update(
                dynamic_range,
                values,
                value_input_option="USER_ENTERED"
            )
            self.retry_api_call(update_func)
            
            logging.info(f"✓ Vendas Vendedor data uploaded successfully to Google Sheets")
            
        except gspread.exceptions.APIError as e:
            raise GoogleSheetsError(f"Google Sheets API error: {str(e)}")
        except Exception as e:
            raise GoogleSheetsError(f"Failed to upload Vendas Vendedor data: {str(e)}")


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
        time.sleep(10)
        
        # Process Excel file
        processor = VendasVendedorProcessor(directory=download_dir)
        latest_file = processor.get_latest_file()
        
        # Convert .xls to .xlsx if needed
        if latest_file.lower().endswith(".xls"):
            latest_file = processor.convert_xls_to_xlsx(latest_file)
        
        # Process the file
        processed_df = processor.process_excel_data(latest_file)
        
        # Show preview
        logging.info("\n=== VENDAS VENDEDOR DATA PREVIEW ===")
        logging.info(f"Total vendedores processed: {len(processed_df)}")
        logging.info(f"Columns before rename: {list(processed_df.columns)}")
        
        if not processed_df.empty:
            logging.info("\nFirst 5 rows (before column rename):")
            logging.info(processed_df.head().to_string())
        
        # Upload to Google Sheets
        uploader = VendasVendedorSheetsUploader(gsa_credentials, sheet_id)
        uploader.authenticate()
        uploader.upload_vendas_vendedor(processed_df, "VENDAS_VENDEDOR")
        
        # Clean up: remove the processed file
        logging.info(f"Removing processed file: {os.path.basename(latest_file)}")
        os.remove(latest_file)
        
        logging.info("✓ Process completed successfully")
        return 0  # Success exit code
        
    except (VendasVendedorError, GoogleSheetsError) as e:
        logging.error(f"✗ {e.__class__.__name__}: {str(e)}")
        return 1  # Business logic failure
    except Exception as e:
        logging.error(f"✗ Unexpected error: {str(e)}")
        return 2  # Unexpected failure


if __name__ == "__main__":
    # Exit with proper code for GitHub Actions
    exit_code = main()
    exit(exit_code)
