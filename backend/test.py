import os

def list_python_only(startpath='.'):
    # Dossiers à ignorer (env virtuels, db, cache)
    exclude = {'.git', '__pycache__', 'venv', '.venv', 'env', 'node_modules', 'data', 'postgres_data'}
    
    for root, dirs, files in os.walk(startpath):
        dirs[:] = [d for d in dirs if d not in exclude and not d.startswith('.')]
        
        # On ne traite que les dossiers qui contiennent au moins un fichier .py
        python_files = [f for f in files if f.endswith('.py')]
        
        if python_files:
            level = root.replace(startpath, '').count(os.sep)
            indent = '    ' * level
            print(f"{indent}📂 {os.path.basename(root) or '.'}/")
            for f in python_files:
                print(f"{indent}    📄 {f}")

if __name__ == "__main__":
    list_python_only()