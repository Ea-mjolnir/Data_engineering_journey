#!/bin/bash

# Script to create .gitkeep files in the existing week3_capstone structure

echo "🔍 Creating .gitkeep files in week3_capstone..."

# Navigate into the project directory (if you're not already there)
# cd week3_capstone  # Uncomment if you need to cd into it

# Create .gitkeep files in the existing directories
touch week3_capstone/data/sample/.gitkeep
touch week3_capstone/data/local/.gitkeep
touch week3_capstone/logs/.gitkeep

# Optional: Add .gitkeep to src subdirectories if you want to keep them empty too
touch week3_capstone/src/extractors/.gitkeep
touch week3_capstone/src/transformers/.gitkeep
touch week3_capstone/src/loaders/.gitkeep
touch week3_capstone/src/utils/.gitkeep
touch week3_capstone/config/.gitkeep
touch week3_capstone/tests/.gitkeep
touch week3_capstone/scripts/.gitkeep
touch week3_capstone/docs/.gitkeep

echo "✅ .gitkeep files created successfully!"
echo ""
echo "📁 .gitkeep files added to:"
ls -la week3_capstone/data/sample/.gitkeep \
      week3_capstone/data/local/.gitkeep \
      week3_capstone/logs/.gitkeep 2>/dev/null \
      || echo "Check if directories exist"
