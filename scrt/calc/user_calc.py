import os
import json
import logging
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread

# Configure logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

class GoogleSheetsError(Exception):
    """Custom exception for Google Sheets operations"""
    pass

class DataCombiner:
    def __init__(self, credentials_json: str, sheet_id: str):
        if not credentials_json:
            raise GoogleSheetsError("Google credentials JSON cannot be empty")
        if not sheet_id:
            raise GoogleSheetsError("Sheet ID cannot be empty")
        
        self.credentials_json = credentials_json
        self.sheet_id = sheet_id
        self.creds = None
        self.client = None
        self.service = None
        
    def authenticate(self):
        """Authenticate with Google Sheets API"""
        try:
            logging.info("Authenticating with Google Sheets API...")
            creds_dict = json.loads(self.credentials_json)
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            self.creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            self.client = gspread.authorize(self.creds)
            self.service = build("sheets", "v4", credentials=self.creds)
            logging.info("✓ Authentication successful")
        except json.JSONDecodeError as e:
            raise GoogleSheetsError(f"Invalid JSON in credentials: {str(e)}")
        except Exception as e:
            raise GoogleSheetsError(f"Authentication failed: {str(e)}")
    
    def get_sheet_data(self, worksheet_name: str) -> pd.DataFrame:
        """Get data from a specific worksheet as DataFrame"""
        try:
            logging.info(f"Fetching data from worksheet: {worksheet_name}")
            spreadsheet = self.client.open_by_key(self.sheet_id)
            worksheet = spreadsheet.worksheet(worksheet_name)
            
            # Get all data
            data = worksheet.get_all_values()
            
            if len(data) <= 1:  # Only header or empty
                raise GoogleSheetsError(f"Worksheet '{worksheet_name}' is empty or has no data")
            
            # Create DataFrame
            df = pd.DataFrame(data[1:], columns=data[0])
            
            # Clean column names
            df.columns = df.columns.str.strip()
            
            logging.info(f"✓ Loaded {len(df)} rows from '{worksheet_name}'")
            logging.info(f"  Columns: {list(df.columns)}")
            
            return df
            
        except gspread.exceptions.WorksheetNotFound:
            raise GoogleSheetsError(f"Worksheet '{worksheet_name}' not found")
        except Exception as e:
            raise GoogleSheetsError(f"Failed to fetch data from '{worksheet_name}': {str(e)}")
    
    def combine_data(self, sci_worksheet: str = "user_sci", trier_worksheet: str = "user_sci") -> pd.DataFrame:
        """Combine data from SCI and Trier worksheets using CPF as key"""
        try:
            logging.info("Starting data combination process...")
            
            # Get data from both worksheets
            sci_df = self.get_sheet_data(sci_worksheet)
            trier_df = self.get_sheet_data(trier_worksheet)
            
            # Prepare SCI data
            # Clean CPF column in SCI data (remove non-numeric characters)
            if 'CPF' in sci_df.columns:
                sci_df['CPF_clean'] = sci_df['CPF'].astype(str).str.replace(r'\D', '', regex=True)
                sci_df = sci_df.drop(columns=['CPF']).rename(columns={'CPF_clean': 'CPF'})
            else:
                raise GoogleSheetsError("'CPF' column not found in SCI worksheet")
            
            # Prepare Trier data
            # Clean CPF column in Trier data (remove non-numeric characters)
            if 'CPF' in trier_df.columns:
                trier_df['CPF_clean'] = trier_df['CPF'].astype(str).str.replace(r'\D', '', regex=True)
                trier_df = trier_df.drop(columns=['CPF']).rename(columns={'CPF_clean': 'CPF'})
            else:
                # If Trier doesn't have CPF, check if it has another identifier
                logging.warning("'CPF' column not found in Trier worksheet")
                # If no CPF, we can't merge - create empty combined dataset
                return self._create_empty_combined_df(sci_df, trier_df)
            
            # Filter out empty CPF values
            sci_df = sci_df[sci_df['CPF'].str.strip() != '']
            trier_df = trier_df[trier_df['CPF'].str.strip() != '']
            
            # Ensure CPF is string for merging
            sci_df['CPF'] = sci_df['CPF'].astype(str)
            trier_df['CPF'] = trier_df['CPF'].astype(str)
            
            # Log CPF overlap
            sci_cpfs = set(sci_df['CPF'])
            trier_cpfs = set(trier_df['CPF'])
            common_cpfs = sci_cpfs.intersection(trier_cpfs)
            
            logging.info(f"Unique CPFs in SCI: {len(sci_cpfs)}")
            logging.info(f"Unique CPFs in Trier: {len(trier_cpfs)}")
            logging.info(f"Common CPFs (will be merged): {len(common_cpfs)}")
            
            # Merge data on CPF
            merged_df = pd.merge(
                sci_df,
                trier_df,
                on='CPF',
                how='inner',  # Only keep rows with matching CPF in both datasets
                suffixes=('_sci', '_trier')
            )
            
            if merged_df.empty:
                logging.warning("No matching CPFs found between datasets")
                # Return empty combined dataset with correct structure
                return self._create_empty_combined_df(sci_df, trier_df)
            
            # Rename and select columns for final output
            # Map columns to desired output format
            column_mapping = {}
            
            # Determine Filial source (prefer SCI if available)
            if 'Filial_sci' in merged_df.columns:
                column_mapping['Filial'] = 'Filial_sci'
            elif 'Filial_trier' in merged_df.columns:
                column_mapping['Filial'] = 'Filial_trier'
            
            # Determine Nome source (prefer SCI if available)
            if 'Nome_sci' in merged_df.columns:
                column_mapping['Nome'] = 'Nome_sci'
            elif 'Nome_trier' in merged_df.columns:
                column_mapping['Nome'] = 'Nome_trier'
            elif 'Funcionário_sci' in merged_df.columns:  # Alternative column name
                column_mapping['Nome'] = 'Funcionário_sci'
            elif 'Funcionário_trier' in merged_df.columns:  # Alternative column name
                column_mapping['Nome'] = 'Funcionário_trier'
            
            # Determine Cargo atual source (prefer SCI if available)
            if 'Cargo atual_sci' in merged_df.columns:
                column_mapping['Cargo atual'] = 'Cargo atual_sci'
            elif 'Cargo atual_trier' in merged_df.columns:
                column_mapping['Cargo atual'] = 'Cargo atual_trier'
            
            # Determine Código source (from Trier)
            if 'Código_trier' in merged_df.columns:
                column_mapping['Código'] = 'Código_trier'
            elif 'Código_sci' in merged_df.columns:
                column_mapping['Código'] = 'Código_sci'
            
            # Keep CPF
            column_mapping['CPF'] = 'CPF'
            
            # Create final DataFrame with desired column order
            desired_columns = ['Filial', 'Código', 'CPF', 'Nome', 'Cargo atual']
            final_df = pd.DataFrame()
            
            for col in desired_columns:
                if col in column_mapping and column_mapping[col] in merged_df.columns:
                    final_df[col] = merged_df[column_mapping[col]]
                else:
                    final_df[col] = None  # Empty column if not found
                    logging.warning(f"Column '{col}' not found in merged data")
            
            # Clean and format the data
            # Remove duplicates based on CPF
            final_df = final_df.drop_duplicates(subset=['CPF'])
            
            # Sort by Filial, then Código
            if 'Filial' in final_df.columns and 'Código' in final_df.columns:
                # Convert to numeric for proper sorting
                final_df['Filial_numeric'] = pd.to_numeric(final_df['Filial'], errors='coerce')
                final_df['Código_numeric'] = pd.to_numeric(final_df['Código'], errors='coerce')
                final_df = final_df.sort_values(['Filial_numeric', 'Código_numeric'])
                final_df = final_df.drop(columns=['Filial_numeric', 'Código_numeric'])
            
            # Reset index
            final_df = final_df.reset_index(drop=True)
            
            logging.info(f"✓ Successfully combined data. Final rows: {len(final_df)}")
            logging.info(f"✓ Final columns: {list(final_df.columns)}")
            
            return final_df
            
        except Exception as e:
            raise GoogleSheetsError(f"Failed to combine data: {str(e)}")
    
    def _create_empty_combined_df(self, sci_df: pd.DataFrame, trier_df: pd.DataFrame) -> pd.DataFrame:
        """Create empty combined DataFrame with correct structure when no merge is possible"""
        desired_columns = ['Filial', 'Código', 'CPF', 'Nome', 'Cargo atual']
        empty_df = pd.DataFrame(columns=desired_columns)
        logging.warning("Created empty combined DataFrame (no CPF matches found)")
        return empty_df
    
    def create_or_update_worksheet(self, df: pd.DataFrame, worksheet_name: str = "filtered_user"):
        """Create or update worksheet with combined data"""
        if df.empty:
            logging.warning(f"DataFrame is empty. Creating empty worksheet '{worksheet_name}'")
        
        try:
            # Get spreadsheet
            spreadsheet = self.client.open_by_key(self.sheet_id)
            
            # Check if worksheet already exists
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                logging.info(f"Worksheet '{worksheet_name}' already exists. Updating...")
            except gspread.exceptions.WorksheetNotFound:
                # Create new worksheet
                logging.info(f"Creating new worksheet: {worksheet_name}")
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=10)
            
            # Clear existing data
            worksheet.clear()
            
            # Prepare data for upload
            # Ensure all columns are present (add missing ones as empty)
            desired_columns = ['Filial', 'Código', 'CPF', 'Nome', 'Cargo atual']
            for col in desired_columns:
                if col not in df.columns:
                    df[col] = None
            
            # Reorder columns
            df = df[desired_columns]
            
            # Fill NaN with empty strings
            df = df.fillna('')
            
            # Convert to list of lists
            values = [df.columns.tolist()] + df.values.tolist()
            
            # Upload data
            logging.info(f"Uploading {len(df)} rows to worksheet '{worksheet_name}'...")
            self.service.spreadsheets().values().update(
                spreadsheetId=self.sheet_id,
                range=worksheet_name,
                valueInputOption="RAW",
                body={"values": values}
            ).execute()
            
            logging.info(f"✓ Worksheet '{worksheet_name}' updated successfully")
            
        except Exception as e:
            raise GoogleSheetsError(f"Failed to create/update worksheet: {str(e)}")

def main():
    """Main execution with proper error handling for GitHub Actions"""
    try:
        # Get environment variables
        sheet_id = os.getenv("SOURCE_SHEET_ID")
        gsa_credentials = os.getenv("GSA_CREDENTIALS")
        
        # Validate environment variables
        if not sheet_id:
            raise GoogleSheetsError("SOURCE_SHEET_ID environment variable not set")
        if not gsa_credentials:
            raise GoogleSheetsError("GSA_CREDENTIALS environment variable not set")
        
        logging.info("Environment variables loaded successfully")
        
        # Initialize and authenticate
        combiner = DataCombiner(gsa_credentials, sheet_id)
        combiner.authenticate()
        
        # Combine data from worksheets
        combined_df = combiner.combine_data(
            sci_worksheet="user_sci",
            trier_worksheet="user_sci"
        )
        
        # Show preview
        logging.info("\n=== COMBINED DATA PREVIEW ===")
        logging.info(f"Total rows: {len(combined_df)}")
        if not combined_df.empty:
            logging.info("\nFirst 5 rows:")
            logging.info(combined_df.head().to_string())
        else:
            logging.info("No data to preview (empty DataFrame)")
        
        # Create or update filtered worksheet
        combiner.create_or_update_worksheet(combined_df, "filtered_user")
        
        logging.info("✓ Process completed successfully")
        return 0  # Success exit code
        
    except GoogleSheetsError as e:
        logging.error(f"✗ {e.__class__.__name__}: {str(e)}")
        return 1  # Business logic failure
    except Exception as e:
        logging.error(f"✗ Unexpected error: {str(e)}")
        return 2  # Unexpected failure

if __name__ == "__main__":
    # Exit with proper code for GitHub Actions
    exit_code = main()
    exit(exit_code)
