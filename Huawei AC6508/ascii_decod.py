#!/usr/bin/env python3
import subprocess
import json
import sys

SNMPWALK = "/usr/bin/snmpwalk"

def usage():
    print("Usage:")
    print('  SNMP v2c:')
    print('    ascii_decod.py "2c" <host> <community> <oid>')
    print('  SNMP v3:')
    print('    ascii_decod.py "3" <host> <user> <level> <auth_proto> <priv_proto> <auth_pass> <priv_pass> <oid>')
    sys.exit(1)

if len(sys.argv) == 5 and sys.argv[1] == "2c":
    _, version, host, community, oid = sys.argv
    cmd = [SNMPWALK, "-v2c", "-c", community, "-Cc", host, oid]

elif len(sys.argv) == 10 and sys.argv[1] == "3":
    _, version, host, user, level, auth_proto, priv_proto, auth_pass, priv_pass, oid = sys.argv
    cmd = [
        SNMPWALK,
        "-v3", "-l", level, "-u", user,
        "-a", auth_proto, "-A", auth_pass,
        "-x", priv_proto, "-X", priv_pass,
        "-Cc", host, oid
    ]
else:
    usage()

try:
    output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode()
except Exception:
    print(json.dumps({ "data": [] }))
    sys.exit(0)

result = []

for line in output.strip().splitlines():
    if "=" not in line:
        continue

    oid_full = line.split(" = ")[0].strip()
    if oid_full.startswith("iso."):
        oid_full = oid_full.replace("iso.", "1.", 1)

    if not oid_full.startswith(oid + "."):
        continue

    try:
        index = oid_full[len(oid) + 1:]
        parts = index.split(".")
        length = int(parts[0])
        ascii_bytes = parts[1:length+1]
        ssid = ''.join([chr(int(x)) for x in ascii_bytes])
        result.append({
            "{#SSID}": ssid,
            "{#OIDINDEX}": index
        })
    except Exception:
        continue

print(json.dumps({ "data": result }, indent=2))
