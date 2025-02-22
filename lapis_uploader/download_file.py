import requests
import os
import zipfile

from dotenv import load_dotenv
import shutil

load_dotenv()

def download_file(file_name,file_url):
    print(file_url)
    token= 'u2vBFA7U-cVRlibx7aFoMCBID7sRXZRRwgGg'
    session_name= 's%3ADdW_dtffMJF_HwtVZ4aNTXpsw9AL1ldK.j77zq0P2FxeZMalfDPzca97LAXQvGK0wSqzEJlE4E0A'

    headers_overleaf = {
    "accept":"application/json",
    "accept-encoding":"gzip, deflate, br",
    "accept-language":"en-US,en;q=0.9",
    "cache-control":"no-cache",
    "content-type":"application/json",
    "cookie":"oa=0; overleaf_session2="+session_name+"; GCLB=CMao2Lr3_OXGDhAD",
    "dnt":"1",
    "pragma":"no-cache",
    "referer":"https://www.overleaf.com/project",
    "sec-ch-ua":"'Google Chrome';v='111', 'Not(A:Brand';v='8', 'Chromium';v='111'",
    "sec-ch-ua-mobile":"?1",
    "sec-ch-ua-platform":"Android",
    "sec-fetch-dest":"empty",
    "sec-fetch-mode":"cors",
    "sec-fetch-site":"same-origin",
    "user-agent":"Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Mobile Safari/537.36",
    "x-csrf-token": token
    }

    r = requests.get(url=f"{file_url}/download/zip",headers=headers_overleaf)

    
    if r.status_code != 200:
        print(r.content)
        raise Exception(f"Unable To download file, Please Update the token for overleaf. response {r.text}")
    
    with open(f'{file_name}.zip', 'wb') as file:
        file.write(r.content)

    if os.path.exists(f"lapis_uploader/overleaf_files/{file_name}"):
        shutil.rmtree(f"lapis_uploader/overleaf_files/{file_name}")
        
    with zipfile.ZipFile(f'{file_name}.zip','r') as zip:
        zip.extractall(f"lapis_uploader/overleaf_files/{file_name}")

    
    os.remove(f'{file_name}.zip')

if __name__ == '__main__':
    download_file(file_name="C6M",file_url="https://www.overleaf.com/project/65179b5ff337f4ece82106a8")


