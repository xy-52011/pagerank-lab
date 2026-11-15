"""生成 PageRank 实验数据集（网页链接图）。

输出格式：每行 "网页<TAB>该网页链接到的网页集合（英文逗号分隔）"。
无出链的网页（悬挂节点）输出 "网页<TAB>"（制表符后为空），用于检验算法对
dangling node 的处理。图的生成采用带优先连接的随机模型（度分布呈长尾），
保证存在明显的枢纽网页，便于观察 PR 值的分化。
"""
import random

TITLES = [
    "Aardvark", "Absinthe", "Abyssinia", "Academy_Award", "Achilles",
    "Ada_Lovelace", "Adaptation", "Adelaide", "Aeschylus", "Aesop",
    "Africa", "Agatha_Christie", "Airbus", "Alaska", "Albert_Einstein",
    "Algebra", "Algorithm", "Alice_in_Wonderland", "Amsterdam", "Anatomy",
    "Andromeda", "Angkor_Wat", "Animation", "Anthropology", "Apache_Hadoop",
    "Apple_Inc", "Architecture", "Aristotle", "Arithmetic", "Art_Deco",
    "Artificial_intelligence", "Astronomy", "Athens", "Atlas", "Augustus",
    "Australia", "Automobile", "Aviation", "Babylon", "Bacteria",
    "Ballet", "Bank", "Basketball", "Beethoven", "Bengal",
    "Bible", "Big_Bang", "Biochemistry", "Biology", "Bird",
    "Bitcoin", "Black_hole", "Blood", "Bollywood", "Book",
    "Botany", "Brain", "Brazil", "Bread", "Buddhism",
    "Calculus", "California", "Camera", "Cancer", "Capitalism",
    "Carbon", "Cartography", "Cat", "Cell_(biology)", "Chemistry",
    "Chess", "Chicago", "China", "Chocolate", "Cinema_of_the_United_States",
    "Civil_engineering", "Climate", "Coffee", "Comedy", "Computer_science",
    "Constitution", "Cotton", "Cricket", "Cryptography", "Cuisine",
    "Dance", "Darwinism", "Database", "Democracy", "Dentistry",
    "Desert", "Dictionary", "Dinosaur", "DNA", "Doctor_Who",
    "Dog", "Drama", "Earthquake", "Ecology", "Economy",
]

DANGLING_INDEX = {3, 17, 28, 45, 59, 73, 88, 95}


def build():
    random.seed(20261115)
    n = len(TITLES)
    indeg = {t: 1 for t in TITLES}
    edges = {}
    for i, page in enumerate(TITLES):
        if i in DANGLING_INDEX:
            edges[page] = []
            continue
        k = random.choice([1, 2, 2, 3, 3, 3, 4, 4, 5, 6])
        targets = []
        weights = [indeg[t] for t in TITLES]
        for _ in range(k):
            tgt = random.choices(TITLES, weights=weights, k=1)[0]
            if tgt != page and tgt not in targets:
                targets.append(tgt)
                indeg[tgt] += 2
        edges[page] = targets
    return edges


def main():
    edges = build()
    lines = []
    for page in TITLES:
        lines.append("%s\t%s" % (page, ",".join(edges[page])))
    with open("pages.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    total_edges = sum(len(v) for v in edges.values())
    dangling = sum(1 for v in edges.values() if not v)
    linked = set(t for v in edges.values() for t in v)
    print("nodes=%d edges=%d dangling=%d 出链为0的网页=%s 未被引用的网页=%d"
          % (len(edges), total_edges, dangling, dangling, len(set(edges) - linked)))


if __name__ == "__main__":
    main()
