import math
import pandas as pd
from math import prod
from pprint import pprint

class OLA:
    def __init__(self, quasi_identifiers, max_equivalence_classes, doubling_step=2):
        self.quasi_identifiers = quasi_identifiers
        self.max_equivalence_classes = max_equivalence_classes
        self.doubling_step = doubling_step
        self.tree = []  # Tree represented as a list of nodes
        self.smallest_passing_ri = None
        self.node_status = {}  # Track node status: 'pass', 'fail', or None

    def calculate_equivalence_classes(self, bin_widths):
        num_equivalence_classes = prod(
            [
                math.ceil(qi.get_range()) / math.ceil(bin_width)
                for qi, bin_width in zip(self.quasi_identifiers, bin_widths)
            ]
        )
        return math.ceil(num_equivalence_classes)

    def build_tree(self):
        max_values = [qi.get_range() for qi in self.quasi_identifiers]
        base = [1] * len(self.quasi_identifiers)
        self.tree = [[base]]
        self.node_status = {tuple(base): None}

        level = 0
        while True:
            next_level = []
            for node in self.tree[level]:
                for i in range(len(node)):
                    new_node = node.copy()
                    if new_node[i] < max_values[i]:
                        new_node[i] = min(new_node[i] * self.doubling_step, max_values[i])
                        if tuple(new_node) not in self.node_status:
                            next_level.append(new_node)
                            self.node_status[tuple(new_node)] = None
            if not next_level:
                break
            self.tree.append(next_level)
            level += 1

        return self.tree

    def find_smallest_passing_ri(self):
        while any(status is None for status in self.node_status.values()):
            unmarked_levels = [
                level for level in range(len(self.tree)) 
                if any(self.node_status.get(tuple(node)) is None for node in self.tree[level])
            ]
            
            if not unmarked_levels:
                break
            
            mid_level = unmarked_levels[len(unmarked_levels) // 2]
            
            sorted_nodes = sorted(
                [node for node in self.tree[mid_level] 
                 if self.node_status.get(tuple(node)) is None]
            )
            
            for node in sorted_nodes:
                if self.node_status.get(tuple(node)) is not None:
                    continue
                
                node_classes = self.calculate_equivalence_classes(node)
                if node_classes <= self.max_equivalence_classes:
                    self._mark_subtree_pass(node, node_classes)
                else:
                    self._mark_parents_fail(node)
        
        self._find_smallest_passing_node()
        
        return self.smallest_passing_ri

    def _mark_subtree_pass(self, node, node_classes):
        self.node_status[tuple(node)] = 'pass'
        
        if not self.smallest_passing_ri or node < self.smallest_passing_ri:
            self.smallest_passing_ri = node
        
        current_level_index = self.tree.index(
            [n for n in self.tree if node in n][0]
        )
        
        for next_level_index in range(current_level_index + 1, len(self.tree)):
            for child_node in self.tree[next_level_index]:
                if all(child_node[i] >= node[i] for i in range(len(node))):
                    if self.node_status.get(tuple(child_node)) is None:
                        child_classes = self.calculate_equivalence_classes(child_node)
                        if child_classes <= self.max_equivalence_classes:
                            self.node_status[tuple(child_node)] = 'pass'
                            if not self.smallest_passing_ri or child_node < self.smallest_passing_ri:
                                self.smallest_passing_ri = child_node

    def _mark_parents_fail(self, node):
        self.node_status[tuple(node)] = 'fail'
        
        current_level_index = -1
        for level_index, level_nodes in enumerate(self.tree):
            if node in level_nodes:
                current_level_index = level_index
                break
        
        while current_level_index > 0:
            current_level_index -= 1
            for parent_node in self.tree[current_level_index]:
                if all(parent_node[i] <= node[i] for i in range(len(node))):
                    if self.node_status.get(tuple(parent_node)) is None:
                        self.node_status[tuple(parent_node)] = 'fail'

    def _find_smallest_passing_node(self):
        if not self.smallest_passing_ri:
            for level in reversed(range(len(self.tree))):
                sorted_nodes = sorted(
                    [node for node in self.tree[level] 
                     if self.node_status.get(tuple(node)) == 'pass']
                )
                if sorted_nodes:
                    self.smallest_passing_ri = sorted_nodes[0]
                    break

    def print_marked_tree(self):
        print("\nMarked Tree Structure:")
        for level, nodes in enumerate(self.tree):
            print(f"Level {level}:")
            for node in nodes:
                status = self.node_status.get(tuple(node), 'unmarked')
                marker = ' <-- Smallest Passing RI' if node == self.smallest_passing_ri else ''
                print(f"  {node} - Status: {status}{marker}")
        
        print(f"\nSmallest Passing RI: {self.smallest_passing_ri}")

    def print_tree(self):
        print("Tree built during OLA:")
        for level, nodes in enumerate(self.tree):
            print(f"Level {level}: {nodes}")

    def process_chunk(self, chunk, bin_widths):
        equivalence_classes = {}

        for _, row in chunk.iterrows():
            key = tuple(
                f"[{(row[qi.column_name] // bin_width) * bin_width}-"
                f"{((row[qi.column_name] // bin_width) + 1) * bin_width - 1}]"
                for qi, bin_width in zip(self.quasi_identifiers, bin_widths)
            )
            equivalence_classes[key] = equivalence_classes.get(key, 0) + 1

        return equivalence_classes

    def merge_histograms(self, histograms):
        global_histogram = {}
        for histogram in histograms:
            for key, count in histogram.items():
                global_histogram[key] = global_histogram.get(key, 0) + count
        return global_histogram

    def check_k_anonymity(self, histogram, k):
        return all(count >= k for count in histogram.values())

    def generalize_chunk(self, chunk, bin_widths):
        generalized_chunk = chunk.copy()

        for qi, bin_width in zip(self.quasi_identifiers, bin_widths):
            col = qi.column_name
            generalized_chunk[col] = generalized_chunk[col].apply(
                lambda x: f"[{(x // bin_width) * bin_width}-{((x // bin_width) + 1) * bin_width - 1}]"
            )

        return generalized_chunk

    def combine_generalized_chunks_to_csv(self, generalized_chunks, output_path='generalized_data.csv'):
        combined_df = pd.concat(generalized_chunks, ignore_index=True)
        combined_df.to_csv(output_path, index=False)
        print(f"Generalized data saved to {output_path}")
        return combined_df

    def re_run_ola_with_histogram(self, initial_ri, global_histogram, k):
        self.tree = [[initial_ri]]  # Start tree with initial Ri
        self.smallest_passing_ri = None

        while True:
            next_level = []
            for node in self.tree[-1]:
                for i in range(len(node)):
                    new_node = node.copy()
                    max_values = [qi.get_range() for qi in self.quasi_identifiers]
                    if new_node[i] < max_values[i]:
                        new_node[i] = min(new_node[i] * self.doubling_step, max_values[i])
                        if new_node not in next_level:
                            next_level.append(new_node)

                            # Check K-anonymity
                            simulated_histogram = self.simulate_histogram(global_histogram, new_node)
                            if self.check_k_anonymity(simulated_histogram, k):
                                self.smallest_passing_ri = new_node

            if not next_level or self.smallest_passing_ri:
                break
            self.tree.append(next_level)

        return self.smallest_passing_ri

    def simulate_histogram(self, global_histogram, bin_widths):
        new_histogram = {}
        for eq_class, count in global_histogram.items():
            numeric_values = []
            for key in eq_class:
                try:
                    if isinstance(key, str):
                        if key.startswith('[') and '-' in key:
                            numeric_val = float(key.split('-')[0][1:])
                        else:
                            numeric_val = float(key)
                    else:
                        numeric_val = float(key)
                    
                    numeric_values.append(numeric_val)
                except (ValueError, TypeError):
                    raise ValueError(f"Cannot parse key: {key}")

            new_eq_class = tuple(
                math.floor(value / bin_width) * bin_width
                for value, bin_width in zip(numeric_values, bin_widths)
            )
            new_histogram[new_eq_class] = new_histogram.get(new_eq_class, 0) + count

        return new_histogram