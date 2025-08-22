import os, sys
from PyInstaller.utils.hooks import collect_dynamic_libs

# Collect the dynamic libraries required by polars
polars_binaries = collect_dynamic_libs('polars')

# Get the current working directory
current_dir = r"C:\Users\hkumar\PycharmProjects\BatchFileTesting\app"
dist_path = os.path.join(current_dir, 'inno', 'dist')
build_path = os.path.join(current_dir, 'inno', 'build')


a = Analysis(
    ['run.py'],
    pathex=[current_dir],  # Use current working directory
    binaries=polars_binaries,  # Include polars binaries here
    datas=[],
    hiddenimports=['openpyxl', 'polars', 'psycopg2', 'pyodbc'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,  # This tells PyInstaller to exclude other binaries, but we're including polars
    name='ClarifiIntegriTest',
    debug=True,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,  # This ensures no console window opens (windowed)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='ClarifiIntegriTest',
    distpath=dist_path,  # Ensure COLLECT places files in the same distpath
	workpath=build_path,
)
