import json

# Leer el JSON generado
data = json.load(open("test_filter_build.json"))

# Obtener nodos y edges
nodes = data["pipelineGraph"]["nodes"]
edges = data["pipelineGraph"]["edges"]

# Buscar el nodo de filtro
filter_node = [n for n in nodes if "Filter" in n["name"]][0]

print("=== ANÁLISIS DE supportedDataRelations ===\n")

print("1. Node FilterTransformStep:")
print(f"   supportedDataRelations: {filter_node.get('supportedDataRelations')}")

print("\n2. Edges en el JSON:")
for e in edges:
    print(f"   {e['origin']} -> {e['destination']}: dataType={e['dataType']}")

print("\n3. Disponibles en DataRelation enum:")
from py2rocket.core.pipeline import DataRelation

for attr in DataRelation:
    print(f"   - {attr.name}: {attr.value}")

print("\n4. Nodos con múltiples supportedDataRelations en JSON:")
multi_relations = [n for n in nodes if len(n.get("supportedDataRelations", [])) > 1]
for n in multi_relations:
    print(f"   - {n['name']}: {n.get('supportedDataRelations')}")
