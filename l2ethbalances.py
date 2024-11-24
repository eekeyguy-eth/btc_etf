from web3 import Web3
from typing import Dict, List
import asyncio
import json
from datetime import datetime
from io import StringIO
import csv
import requests

# Initialize Infura connection
INFURA_URL = "https://mainnet.infura.io/v3/632302059d944e88a5999097f1d0ec36"
w3 = Web3(Web3.HTTPProvider(INFURA_URL))

# Chain addresses mapping (using the same ADDRESSES dict as in your code)
ADDRESSES = {
     'arbitrum': '0x8315177aB297bA92A06054cE80a67Ed4DBd7ed3a',
    'base': '0x49048044D57e1C92A77f79988d21Fa8fAF74E97e',
    'optimism': '0xbEb5Fc579115071764c7423A4f12eDde41f106Ed',
    'blast': '0x98078db053902644191f93988341E31289E1C8FE',
    'zksync era': '0xD7f9f54194C633F36CCD5F3da84ad4a1c38cB2cB',
    'linea': '0xd19d4B5d358258f05D7B411E21A1460D11B0876F',
    'scroll': '0x6774Bcbd5ceCeF1336b5300fb5186a12DDD8b367',
    'starknet': '0xae0Ee0A63A2cE6BaeEFFE56e7714FB4EFE48D419',
    'mode': '0x8B34b14c7c7123459Cf3076b8Cb929BE097d0C07',
    'fuel': '0xAEB0c00D0125A8a788956ade4f4F12Ead9f65DDf',
    'world chain': '0xd5ec14a83B7d95BE1E2Ac12523e2dEE12Cbeea6C',
    'lisk': '0x26dB93F8b8b4f7016240af62F7730979d353f9A7',
    'zircuit': '0x17bfAfA932d2e23Bd9B909Fd5B4D2e2a27043fb1',
    'bob': '0x8AdeE124447435fE03e3CD24dF3f4cAE32E65a3E',
    'taiko': '0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC',
    'polygon zkevm': '0x2a3DD3EB832aF982ec71669E178424b10Dca2EDe',
    'kinto': ['0x859a53Fe2C8DA961387030E7CB498D6D20d0B2DB', '0x0f1b7bd7762662B23486320AA91F30312184f70C'],
    'zksync lite': '0xaBEA9132b05A70803a4E85094fD0e1800777fBEF',
    'loopring': '0x674bdf20A0F284D710BC40872100128e2d66Bd3f',
    'degate v1': '0x54D7aE423Edb07282645e740C046B9373970a168',
    'zora': '0x1a0ad011913A150f69f6A19DF447A0CfD9551054',
    'boba': '0x7B02D13904D8e6E0f0Efaf756aB14Cb0FF21eE7e',
    'kroma': '0x31F648572b67e60Ec6eb8E197E1848CC5F5558de',
    'morph': '0xDc71366EFFA760804DCFC3EDF87fa2A6f1623304',
    'polynomial': '0x034cbb620d1e0e4C2E29845229bEAc57083b04eC',
    'mint': '0x59625d1FE0Eeb8114a4d13c863978F39b3471781',
    'shape': '0xEB06fFa16011B5628BaB98E29776361c83741dd3',
    'zkspace': '0x5CDAF83E077DBaC2692b5864CA18b61d67453Be8',
    'debank chain': '0x63CA00232F471bE2A3Bf3C4e95Bc1d2B3EA5DB92',
    'optopia': '0x39A90926306E11497EC5FE1C459910258B620edD',
    'swan chain': '0xBa50434BC5fCC07406b1baD9AC72a4CDf776db15',
    'superlumio': '0x9C93982cb4861311179aE216d1B7fD61232DE1f0',
    'metal': '0x3F37aBdE2C6b5B2ed6F8045787Df1ED1E3753956',
    'parallel': '0x5a961c7D162195a9Dc5a357Cc168b0694283382E',
    'river': '0x9fDEEa19836A413C04e9672d3d09f482278e863c',
    'race network': '0x0485Ca8A73682B3D3f5ae98cdca1E5b512E728e9',
    'frame chain': '0x52fA397D799f1CE416a2089B964Aa293c347994F',
    'lambda chain': '0x7288e508f56c1b4b52D2e4Fd3688a711c7cE0054',
    'ethernity': '0xDA29f0B4da6c23f6c1aF273945c290C0268c4ea9',
    'hook': '0x6BC4F2698cd385a04ee0B1805D15E995c45476F6',
    'kontos': '0xc08a7164F9E9d8aB66CcB67D49d6FB116b5808dD'
}

async def get_balance_at_block(address: str, block_number: int) -> float:
    try:
        balance_wei = w3.eth.get_balance(address, block_number)
        balance_eth = w3.from_wei(balance_wei, 'ether')
        return float(balance_eth)
    except Exception as e:
        print(f"Error fetching balance for {address} at block {block_number}: {str(e)}")
        return 0.0

async def get_all_balances() -> List[Dict]:
    results = []
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # Get current block number
    current_block = w3.eth.block_number
    blocks_per_day = 24 * 60 * 60 // 12
    block_90d_ago = current_block - int(90 * blocks_per_day)
    block_180d_ago = current_block - int(180 * blocks_per_day)
    
    for chain, address in ADDRESSES.items():
        if isinstance(address, list):
            for addr in address:
                current_balance = await get_balance_at_block(addr, current_block)
                balance_90d = await get_balance_at_block(addr, block_90d_ago)
                balance_180d = await get_balance_at_block(addr, block_180d_ago)
                results.append({
                    'date': current_date,
                    'chain': chain,
                    'address': addr,
                    'current_balance': current_balance,
                    'balance_90d': balance_90d,
                    'balance_180d': balance_180d,
                    'block_number': current_block
                })
        else:
            current_balance = await get_balance_at_block(address, current_block)
            balance_90d = await get_balance_at_block(address, block_90d_ago)
            balance_180d = await get_balance_at_block(address, block_180d_ago)
            results.append({
                'date': current_date,
                'chain': chain,
                'address': address,
                'current_balance': current_balance,
                'balance_90d': balance_90d,
                'balance_180d': balance_180d,
                'block_number': current_block
            })
    
    return results

def convert_to_csv(data: List[Dict]) -> str:
    csv_file = StringIO()
    fieldnames = ['date', 'chain', 'address', 'current_balance', 'balance_90d', 'balance_180d', 'block_number']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)
    return csv_file.getvalue()

def upload_to_dune(csv_data: str):
    dune_upload_url = "https://api.dune.com/api/v1/table/upload/csv"
    payload = {
        "data": csv_data,
        "description": "Chain ETH Balances Tracking",
        "table_name": "chain_eth_balances",
        "is_private": False
    }
    headers = {
        'Content-Type': 'application/json',
        'X-DUNE-API-KEY': 'p0RZJpTPCUn9Cn7UTXEWDhalc53QzZXV'
    }
    
    try:
        response = requests.post(
            dune_upload_url,
            headers=headers,
            data=json.dumps(payload)
        )
        response.raise_for_status()
        print("Successfully uploaded data to Dune")
        print(response.text)
    except Exception as e:
        print(f"Error uploading to Dune: {str(e)}")

async def main():
    # Fetch balances
    print("Fetching balances...")
    results = await get_all_balances()
    
    # Convert to CSV
    print("Converting to CSV format...")
    csv_data = convert_to_csv(results)
    
    # Upload to Dune
    print("Uploading to Dune...")
    upload_to_dune(csv_data)

if __name__ == "__main__":
    asyncio.run(main())
