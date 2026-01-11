import os
import glob

# Characters to replace
replacements = {
    '"': '"',  # Smart quote start
    '"': '"',  # Smart quote end
    ''': "'",  # Smart single quote start
    ''': "'",  # Smart single quote end
    '✓': '[OK]',
    '…': '...',
    '║': '|',
    '╔': '=',
    '╗': '=',
    '╚': '=',
    '╝': '=',
}

setup_dir = r"C:\BK_WORKSPACE\bigdata\bigdata_storage_and_proccess_job_data\bigdata-project\scripts\setup"
os.chdir(setup_dir)

ps1_files = glob.glob("*.ps1")

for file in ps1_files:
    print(f"Fixing {file}...")
    
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Apply all replacements
    for old, new in replacements.items():
        content = content.replace(old, new)
    
    with open(file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"  [OK] Fixed {file}")

print("\nAll files fixed!")
