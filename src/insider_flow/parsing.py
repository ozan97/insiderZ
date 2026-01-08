import os
import re
from lxml import etree
from datetime import datetime
from typing import List, Optional
import fsspec # <--- NEW IMPORT
from .schema import InsiderTrade
from .utils import get_storage_options # <--- Need credentials

# Regex remains the same
XML_REGEX = re.compile(r'<xml.*?>(.*?)</xml>', re.DOTALL | re.IGNORECASE)

def extract_xml_from_text(content: str) -> Optional[str]:
    match = XML_REGEX.search(content)
    if match:
        return match.group(1)
    return None

def parse_filing(file_path: str, filing_date: str) -> List[InsiderTrade]:
    try:
        # Get raw options
        opts = get_storage_options()
        
        # --- Translate for fsspec
        if "google_application_credentials" in opts:
            opts["token"] = opts["google_application_credentials"]
        
        # Now pass these fixed opts to fsspec.open
        with fsspec.open(file_path, mode='r', encoding='utf-8', errors='ignore', **opts) as f:
            content = f.read()

    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

    xml_content = extract_xml_from_text(content)
    if not xml_content:
        return []

    try:
        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(xml_content.encode('utf-8'), parser=parser)
        
        for elem in root.getiterator():
            if not hasattr(elem.tag, 'find'): continue
            i = elem.tag.find('}')
            if i >= 0:
                elem.tag = elem.tag[i+1:]
        
    except Exception as e:
        print(f"XML Parsing failed for {file_path}: {e}")
        return []

    trades = []
    
    def get_text(node, path):
        res = node.xpath(path)
        return res[0].text.strip() if res and res[0].text else None

    ticker = get_text(root, ".//issuerTradingSymbol")
    company_name = get_text(root, ".//issuerName")
    cik = get_text(root, ".//rptOwnerCik")
    owner_name = get_text(root, ".//rptOwnerName")
    owner_title = get_text(root, ".//officerTitle")
    is_director = get_text(root, ".//isDirector") == '1' or get_text(root, ".//isDirector") == 'true'
    is_officer = get_text(root, ".//isOfficer") == '1' or get_text(root, ".//isOfficer") == 'true'
    is_ten_percent = get_text(root, ".//isTenPercentOwner") == '1' or get_text(root, ".//isTenPercentOwner") == 'true'

    filename = file_path.split("/")[-1] # Always split by forward slash for cloud paths
    if '_' in filename:
        accession = filename.split('_')[-1].replace('.txt', '')
    else:
        accession = filename.replace('.txt', '')
    
    transactions = root.xpath(".//nonDerivativeTransaction")
    
    for t in transactions:
        try:
            t_date_str = get_text(t, ".//transactionDate/value")
            t_code = get_text(t, ".//transactionCoding/transactionCode")
            shares_str = get_text(t, ".//transactionAmounts/transactionShares/value")
            price_str = get_text(t, ".//transactionAmounts/transactionPricePerShare/value")
            
            t_shares = float(shares_str) if shares_str else 0.0
            t_price = float(price_str) if price_str else 0.0
            t_ad_code = get_text(t, ".//transactionAmounts/transactionAcquiredDisposedCode/value")
            
            if not t_date_str or t_shares == 0: continue
            if not t_code or t_code.upper() not in ['P', 'S']: continue
            if t_price <= 0.0: continue

            try:
                trans_date = datetime.strptime(t_date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            trade = InsiderTrade(
                cik=cik or "UNKNOWN",
                accession_number=accession,
                filing_date=datetime.strptime(str(filing_date), "%Y-%m-%d").date(),
                ticker=ticker or "UNKNOWN",
                company_name=company_name or "UNKNOWN",
                owner_name=owner_name or "UNKNOWN",
                owner_title=owner_title,
                is_director=is_director,
                is_officer=is_officer,
                is_ten_percent_owner=is_ten_percent,
                transaction_date=trans_date,
                transaction_code=t_code.upper(),
                shares=t_shares,
                price_per_share=t_price,
                total_value=t_shares * t_price,
                acquired_disposed_code=t_ad_code or "A"
            )
            trades.append(trade)
        except Exception:
            continue
            
    return trades