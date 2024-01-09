import requests
import sys

# Checks if the tag of the version starts with `quick_start_`
def is_quick_start(version) -> bool:
    for tag in version['metadata']['container']['tags']:
        if tag.startswith("quick_start_"):
            return True
    
    return False

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please input your token.")
        exit(-1)
    
    token = sys.argv[1]

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    

    url = "https://api.github.com/orgs/datenlord/packages/container/datenlord/versions"

    r = requests.get(url=url, headers=headers)
    
    code = r.status_code
    if code != 200:
        print(f"{code}:", r.text)
        exit(-1)

    json = r.json()

    package_urls = list(map(lambda v: v['url'], filter(is_quick_start, json)))
    print(f"{len(package_urls)} packages found:")
    for package_url in package_urls:
        print(package_url)

    for package_url in package_urls:
        print("Deleteing package:", package_url)
        r = requests.delete(url=package_url, headers=headers)
        code = r.status_code
        if code != 204:
            print(f"{code}:", r.text)
            exit(-1)