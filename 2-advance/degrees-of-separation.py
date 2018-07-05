"""
Implementation of Breadth-First Search Algorithm to find the degrees of separation
from each Superhero
"""
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)

startCharacterID = 5306
targetCharacterID = 14

# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hitCounter = sc.accumulator(0)


def convert_to_bfs(line):
    """
    Parse each line of the file so it can be converted into BFS data set.
    :param line: a line from input file
    :return: a k/v pair wirh heroId and its relation to other superheroes
    """
    fields = line.split()
    hero_id = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    if hero_id == startCharacterID:
        color = 'GRAY'
        distance = 0

    return hero_id, (connections, distance, color)


def create_starting_rdd():
    """
    Reads the input file a parse eac line
    :return:
    """
    input_file = sc.textFile("file:///SparkCourse/Marvel-Graph.txt")
    return input_file.map(convert_to_bfs)


def bfs_map(node):
    character_id = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    # If this node needs to be expanded...
    if color == 'GRAY':
        for connection in connections:
            new_character_id = connection
            new_distance = distance + 1
            new_color = 'GRAY'
            if targetCharacterID == connection:
                hitCounter.add(1)

            new_entry = (new_character_id, ([], new_distance, new_color))
            results.append(new_entry)

        # We've processed this node, so color it black
        color = 'BLACK'

    # Emit the input node so we don't lose it.
    results.append((character_id, (connections, distance, color)))
    return results
