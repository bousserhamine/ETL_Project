import random
import pandas as pd
from datetime import datetime, timedelta

# Définition des listes de valeurs possibles
produits = ['Produit_A', 'Produit_B', 'Produit_C']
régions = ['Agadir', 'Tanger', 'Marrakech', 'Casablanca']

def generate_random_date():
    """Génère une date aléatoire dans les six derniers mois."""
    end_date = datetime.today()
    start_date = end_date - timedelta(days=180)
    random_date = start_date + timedelta(days=random.randint(0, 180))
    return random_date.strftime('%Y-%m-%d')

# Génération des données
ventes_data = []
for _ in range(1000):
    vente = {
        'Date de vente': generate_random_date(),
        'Produit': random.choice(produits),
        'Région': random.choice(régions),
        'Ventes': random.randint(100, 1000)  # Montant sans virgule
    }
    ventes_data.append(vente)

# Création d'un DataFrame
df = pd.DataFrame(ventes_data)

# Sauvegarde en CSV
df.to_csv('ventes.csv', index=False, sep=';')

print("Fichier 'ventes.csv' généré avec succès !")
