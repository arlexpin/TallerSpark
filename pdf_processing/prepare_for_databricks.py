#!/usr/bin/env python3
"""
Script para preparar el proyecto para despliegue en Databricks.

Este script reorganiza los archivos según la estructura esperada por el YAML:
- Mueve los scripts Python a src/pipelines/pdf_processing/
- Verifica que todos los archivos necesarios existan
"""

import os
import shutil
from pathlib import Path

# Estructura esperada
REQUIRED_FILES = {
    "src/pipelines/pdf_processing/pdf_to_text_with_pymupdf4llm.py": "pdf_to_text_with_pymupdf4llm.py",
    "src/pipelines/pdf_processing/text_to_columns.py": "text_to_columns.py",
    "src/pipelines/pdf_processing/text_to_columns_validator.py": "text_to_columns_validator.py",
}

def prepare_structure():
    """Reorganiza los archivos según la estructura de Databricks."""
    base_dir = Path(__file__).parent
    print(f"📁 Directorio base: {base_dir}")
    
    # Crear estructura de directorios
    target_dir = base_dir / "src" / "pipelines" / "pdf_processing"
    target_dir.mkdir(parents=True, exist_ok=True)
    print(f"✅ Directorio creado: {target_dir}")
    
    # Mover/copiar archivos
    moved = []
    missing = []
    
    for target_path, source_file in REQUIRED_FILES.items():
        source = base_dir / source_file
        target = base_dir / target_path
        
        if source.exists():
            if not target.exists():
                shutil.copy2(source, target)
                print(f"✅ Copiado: {source_file} → {target_path}")
                moved.append(source_file)
            else:
                print(f"⚠️  Ya existe: {target_path}")
        else:
            print(f"❌ No encontrado: {source_file}")
            missing.append(source_file)
    
    # Verificar YAML
    yaml_file = base_dir / "pdf_processing_git.yml"
    if yaml_file.exists():
        print(f"✅ YAML encontrado: pdf_processing_git.yml")
    else:
        print(f"❌ YAML no encontrado: pdf_processing_git.yml")
    
    # Resumen
    print("\n" + "="*60)
    print("RESUMEN")
    print("="*60)
    print(f"✅ Archivos copiados: {len(moved)}")
    if missing:
        print(f"❌ Archivos faltantes: {len(missing)}")
        for f in missing:
            print(f"   - {f}")
    else:
        print("✅ Todos los archivos necesarios están presentes")
    
    print("\n📋 Próximos pasos:")
    print("1. Revisar los archivos en src/pipelines/pdf_processing/")
    print("2. Hacer commit y push a tu repositorio Git")
    print("3. Seguir la guía en DEPLOYMENT_GUIDE.md")

if __name__ == "__main__":
    prepare_structure()

