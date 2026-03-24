# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['1.1.9 Enhanced Extract Manual WT.py'],
    pathex=[],
    binaries=[],
    datas=[('wticon.ico', '.')],
    hiddenimports=['secrets'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['scipy', 'matplotlib', 'tkinter', '_tkinter', 'IPython', 'PIL', 'Pillow', 'jedi', 'pygments', 'zmq', 'sqlite3', 'pytest', 'unittest', 'xmlrpc', 'pydoc', 'doctest', 'setuptools', 'psutil', 'certifi', 'PySide6.QtNetwork', 'PySide6.QtQml', 'PySide6.QtQuick', 'PySide6.QtSvg', 'PySide6.QtOpenGL', 'PySide6.QtDBus', 'PySide6.QtMultimedia', 'PySide6.QtBluetooth', 'PySide6.QtWebEngine', 'PySide6.QtWebEngineWidgets', 'PySide6.QtPdf', 'PySide6.Qt3DCore'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='Water_Table_Extractor_v1.1.9',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['wticon.ico'],
)
