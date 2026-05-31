#!/bin/bash
# Script pour nettoyer tous les fichiers __pycache__ et .pyc du repo

echo "🧹 Nettoyage des fichiers Python cache..."

# Supprimer tous les répertoires __pycache__
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null

# Supprimer tous les fichiers .pyc
find . -type f -name "*.pyc" -delete 2>/dev/null

# Supprimer tous les fichiers .pyo
find . -type f -name "*.pyo" -delete 2>/dev/null

# Supprimer tous les fichiers .pyd
find . -type f -name "*.pyd" -delete 2>/dev/null

echo "✅ Nettoyage terminé !"
