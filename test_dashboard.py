#!/usr/bin/env python3
"""
Quick test of the Order Analysis Dashboard
"""

import subprocess
import time
import sys
import os

def test_dashboard():
    """Test the dashboard installation and startup"""
    
    print("=" * 70)
    print("Order Analysis Dashboard - Quick Test")
    print("=" * 70)
    print()
    
    # Check Python version
    print("✓ Python version:", sys.version.split()[0])
    
    # Check Flask
    try:
        import flask
        print("✓ Flask:", flask.__version__)
    except ImportError:
        print("✗ Flask not installed")
        return False
    
    # Check Pandas
    try:
        import pandas
        print("✓ Pandas:", pandas.__version__)
    except ImportError:
        print("✗ Pandas not installed")
        return False
    
    # Check Matplotlib
    try:
        import matplotlib
        print("✓ Matplotlib:", matplotlib.__version__)
    except ImportError:
        print("✗ Matplotlib not installed")
        return False
    
    print()
    print("=" * 70)
    print("All dependencies are installed!")
    print("=" * 70)
    print()
    
    print("Starting Dashboard...")
    print()
    print("📊 Dashboard will be available at: http://localhost:5000")
    print()
    print("Features:")
    print("  • Upload or paste order logs")
    print("  • Auto-detect log formats")
    print("  • View price vs timestamp charts")
    print("  • Export data as CSV")
    print("  • View order statistics")
    print()
    print("Press Ctrl+C to stop the server")
    print()
    print("=" * 70)
    print()
    
    # Start the dashboard
    os.chdir('/home/ubuntu/Muse2_compile')
    subprocess.run(['python3', 'dashboard.py'])

if __name__ == '__main__':
    test_dashboard()
