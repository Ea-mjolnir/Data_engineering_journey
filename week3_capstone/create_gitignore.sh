#!/bin/bash

# Simple script to create a comprehensive .gitignore for a data engineering project

echo "📝 Creating .gitignore file..."

cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*.pyo
*.pyd
.Python
venv/
env/
*.egg-info/
.pytest_cache/

# Data files
data/local/*
data/sample/*.csv
data/sample/*.json
!data/sample/.gitkeep
!data/local/.gitkeep
*.parquet
*.db

# Logs
logs/*
!logs/.gitkeep
*.log

# Environment
.env
.env.*
!.env.example

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Test coverage
.coverage
htmlcov/

# Temporary files
*.tmp
*.bak
*~
EOF

echo "✅ .gitignore created successfully!"
echo "📄 Location: $(pwd)/.gitignore"
