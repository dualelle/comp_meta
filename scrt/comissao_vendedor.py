import os
import time
import logging
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Configure logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

class TrierDownloadError(Exception):
    """Custom exception for Trier download failures"""
    pass

class ComissaoVendedorDownloader:
    def __init__(self, username: str, password: str, download_dir: str = None):
        if not username or not password:
            raise TrierDownloadError("Username and password cannot be empty")
        
        self.username = username
        self.password = password
        self.download_dir = download_dir or os.getcwd()
        self.driver = None
        
        # Calculate date range
        today = datetime.now()
        if today.day == 1:
            # First day of month: get previous month
            mes_anterior = today.replace(day=1) - timedelta(days=1)
            self.data_inicio = mes_anterior.replace(day=1).strftime('%d/%m/%Y')
            self.data_fim = mes_anterior.strftime('%d/%m/%Y')
        else:
            # Not first day: get current month up to yesterday
            self.data_inicio = today.replace(day=1).strftime('%d/%m/%Y')
            self.data_fim = (today - timedelta(days=1)).strftime('%d/%m/%Y')
        
        logging.info(f"Date range configured: {self.data_inicio} to {self.data_fim}")
    
    def setup_driver(self):
        """Configure Chrome driver with download settings"""
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            
            # Configure download behavior
            prefs = {
                "download.default_directory": self.download_dir,
                "download.prompt_for_download": False,
                "directory_upgrade": True,
                "safebrowsing.enabled": False,
                "safebrowsing.disable_download_protection": True
            }
            options.add_experimental_option("prefs", prefs)
            options.add_argument("--unsafely-treat-insecure-origin-as-secure=http://drogcidade.ddns.net:4647/sgfpod1/Login.pod")
            
            self.driver = webdriver.Chrome(options=options)
            self.driver.implicitly_wait(10)
            logging.info("WebDriver initialized successfully")
            
        except Exception as e:
            raise TrierDownloadError(f"Failed to setup WebDriver: {str(e)}")
    
    def login(self):
        """Login to Trier system"""
        try:
            url = "http://drogcidade.ddns.net:4647/sgfpod1/Login.pod"
            logging.info(f"Navigating to {url}")
            self.driver.get(url)
            
            # Enter credentials
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, "id_cod_usuario"))
            ).send_keys(self.username)
            
            self.driver.find_element(By.ID, "nom_senha").send_keys(self.password)
            self.driver.find_element(By.NAME, "login").click()
            
            # Wait for login to complete
            WebDriverWait(self.driver, 20).until(
                lambda d: d.execute_script("return document.readyState === 'complete'")
            )
            time.sleep(5)
            
            # Check for login errors
            if "login" in self.driver.current_url.lower():
                raise TrierDownloadError("Login failed - still on login page")
                
            logging.info("Login successful")
            
        except TimeoutException as e:
            raise TrierDownloadError(f"Login timeout: {str(e)}")
        except Exception as e:
            raise TrierDownloadError(f"Login failed: {str(e)}")
    
    def navigate_to_comissao_vendedores(self):
        """Navigate to Comissão Vendedores report"""
        try:
            # Fullscreen (F11)
            self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.F11)
            time.sleep(2)
            
            # Search and select menu item
            search_box = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.ID, "sideMenuSearch"))
            )
            search_box.send_keys("Comissão Vendedores")
            search_box.click()
            time.sleep(2)
            
            menu_item = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '[title="Comissão Vendedores"]'))
            )
            menu_item.click()
            
            # Wait for page load
            WebDriverWait(self.driver, 20).until(
                lambda d: d.execute_script("return document.readyState === 'complete'")
            )
            logging.info("Navigated to Comissão Vendedores page successfully")
            
        except TimeoutException as e:
            raise TrierDownloadError(f"Navigation timeout: {str(e)}")
        except Exception as e:
            raise TrierDownloadError(f"Navigation failed: {str(e)}")
    
    def configure_report(self):
        """Configure report options and date range"""
        try:
            # Fill date range
            data_inicio_field = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="dat_inicial"]'))
            )
            data_inicio_field.send_keys(self.data_inicio)
            
            data_fim_field = self.driver.find_element(By.XPATH, '//*[@id="dat_fim"]')
            data_fim_field.send_keys(self.data_fim)
            
            logging.info(f"Set date range: {self.data_inicio} to {self.data_fim}")
            
            # Configure grouping
            agrupamento = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="agrup_fil_2"]'))
            )
            agrupamento.click()
            
            # Configure output format
            saida = self.driver.find_element(By.XPATH, '//*[@id="saida_4"]')
            saida.click()
            
            logging.info("Report configuration complete")
            
        except Exception as e:
            raise TrierDownloadError(f"Report configuration failed: {str(e)}")
    
    def download_and_rename_file(self, new_filename: str = "raw_comissao_vendedor.xls") -> str:
        """Trigger download and rename file"""
        try:
            # Start download
            download_button = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable((By.ID, "runReport"))
            )
            download_button.click()
            
            logging.info("Download triggered, waiting for file...")
            time.sleep(15)  # Wait for download
            
            # Find downloaded files
            downloaded_files = [
                f for f in os.listdir(self.download_dir) 
                if f.endswith(('.xls', '.xlsx'))
            ]
            
            if not downloaded_files:
                # Check for .crdownload files (incomplete downloads)
                temp_files = [f for f in os.listdir(self.download_dir) if '.crdownload' in f]
                if temp_files:
                    raise TrierDownloadError("Download still in progress")
                else:
                    raise TrierDownloadError("No XLS/XLSX files found after download")
            
            # Get most recent file
            latest_file = max(
                downloaded_files,
                key=lambda f: os.path.getmtime(os.path.join(self.download_dir, f))
            )
            
            old_path = os.path.join(self.download_dir, latest_file)
            new_path = os.path.join(self.download_dir, new_filename)
            
            # Remove existing file if it exists
            if os.path.exists(new_path):
                os.remove(new_path)
            
            # Rename the file
            import shutil
            shutil.move(old_path, new_path)
            
            file_size = os.path.getsize(new_path)
            if file_size == 0:
                raise TrierDownloadError("Downloaded file is empty (0 bytes)")
            
            logging.info(f"✓ File saved as {new_filename} ({file_size:,} bytes)")
            return new_path
            
        except Exception as e:
            raise TrierDownloadError(f"Download failed: {str(e)}")
    
    def run(self):
        """Main execution flow"""
        try:
            logging.info("Starting Comissão Vendedores download process...")
            self.setup_driver()
            self.login()
            self.navigate_to_comissao_vendedores()
            self.configure_report()
            file_path = self.download_and_rename_file()
            
            logging.info("✓ Report download completed successfully")
            return file_path
            
        except Exception as e:
            logging.error(f"✗ Report download failed: {str(e)}")
            raise  # Re-raise to ensure GitHub Actions sees the failure
            
        finally:
            if self.driver:
                logging.info("Cleaning up WebDriver...")
                self.driver.quit()


def main():
    """Entry point - any uncaught exception here will stop GitHub Actions"""
    try:
        # Get credentials from environment
        username = os.getenv("SGF_USERNAME")  # Using specific name
        password = os.getenv("SGF_PASSWORD")
        
        if not username:
            raise TrierDownloadError("trier_user environment variable not set")
        if not password:
            raise TrierDownloadError("trier_password environment variable not set")
        
        logging.info("Environment variables loaded successfully")
        
        # Create and run downloader
        downloader = ComissaoVendedorDownloader(username, password)
        downloaded_file = downloader.run()
        
        logging.info(f"✓ Success: Report downloaded to {downloaded_file}")
        return 0  # Success exit code
        
    except TrierDownloadError as e:
        logging.error(f"✗ Trier Download Error: {str(e)}")
        return 1  # Business logic failure
    except Exception as e:
        logging.error(f"✗ Unexpected error: {str(e)}")
        return 2  # Unexpected failure


if __name__ == "__main__":
    # Exit with proper code for GitHub Actions
    exit_code = main()
    exit(exit_code)
