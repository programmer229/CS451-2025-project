#!/usr/bin/env python3
import os
import sys
import glob

def parse_config(filepath):
    """
    Parses a Lattice Agreement config file.
    Returns a list of sets, where list[i] is the proposed set for slot i.
    """
    proposals = []
    with open(filepath, 'r') as f:
        lines = [l.strip() for l in f if l.strip()]
        if not lines:
            return []
        
        # First line is metadata: P vs ds (or just P)
        header = list(map(int, lines[0].split()))
        num_proposals = header[0]
        
        # Remaining lines are proposals
        proposal_lines = lines[1:]
        
        for line in proposal_lines:
            # Each line is space-separated integers
            parts = list(map(int, line.split()))
            proposals.append(set(parts))
            
    return proposals

def parse_output(filepath):
    """
    Parses a process output file.
    Returns a list of sets, where list[i] is the decided set for slot i.
    """
    decisions = []
    if not os.path.exists(filepath):
        return []
        
    with open(filepath, 'r') as f:
        for line in f:
            parts = list(map(int, line.split()))
            decisions.append(set(parts))
    return decisions

def check_subset_property(sets):
    """
    Checks if for every pair (A, B) in sets, A <= B or B <= A.
    Returns (True, None) or (False, error_msg)
    """
    for i in range(len(sets)):
        for j in range(i + 1, len(sets)):
            s1 = sets[i]
            s2 = sets[j]
            if not (s1.issubset(s2) or s2.issubset(s1)):
                return False, f"Incomparable sets found: {s1} vs {s2}"
    return True, None

def validate(config_dir, output_dir, num_processes):
    print(f"Validating Lattice Agreement...")
    print(f"Config Dir: {config_dir}")
    print(f"Output Dir: {output_dir}")
    
    # Load Configs
    proc_proposals = {} # pid -> list of sets
    for i in range(1, num_processes + 1):
        cfg_path = os.path.join(config_dir, f"lattice-agreement-{i}.config")
        if not os.path.exists(cfg_path):
            print(f"Error: Missing config file {cfg_path}")
            sys.exit(1)
        proc_proposals[i] = parse_config(cfg_path)
        
    # Load Outputs
    proc_decisions = {} # pid -> list of sets
    for i in range(1, num_processes + 1):
        out_path = os.path.join(output_dir, f"{i}.output")
        proc_decisions[i] = parse_output(out_path)
    
    # Determine Check Range (min number of slots common to configs)
    num_slots = min(len(p) for p in proc_proposals.values())
    if num_slots == 0:
        print("Error: No proposals found in configs.")
        sys.exit(1)
        
    print(f"Checking {num_slots} slots across {num_processes} processes.\n")
    
    all_passed = True
    
    for slot in range(num_slots):
        # 1. Gather Proposals and Decisions for this slot
        curr_proposals = {}
        curr_decisions = {}
        
        union_proposals = set()
        
        for pid in range(1, num_processes + 1):
            # Proposal
            if slot < len(proc_proposals[pid]):
                prop = proc_proposals[pid][slot]
                curr_proposals[pid] = prop
                union_proposals.update(prop)
            
            # Decision
            if pid in proc_decisions and slot < len(proc_decisions[pid]):
                curr_decisions[pid] = proc_decisions[pid][slot]
        
        # Skip if no decisions (e.g. process crashed)
        if not curr_decisions:
            print(f"Slot {slot}: No decisions found.")
            continue
            
        print(f"Slot {slot}: Checking {len(curr_decisions)} decisions.")
        
        # PROPERTY 1: CONSISTENCY (Subset Property)
        decided_values = list(curr_decisions.values())
        consistent, consist_err = check_subset_property(decided_values)
        if not consistent:
            print(f"  [FAIL] Consistency: {consist_err}")
            all_passed = False
        else:
            # print(f"  [PASS] Consistency")
            pass
            
        # PROPERTY 2: VALIDITY (Decided <= Union of Proposed)
        for pid, dec in curr_decisions.items():
            if not dec.issubset(union_proposals):
                diff = dec - union_proposals
                print(f"  [FAIL] Validity (PID {pid}): Decided value contains extra elements {diff}")
                all_passed = False
        
        # PROPERTY 3: SELF-INCLUSION (Proposed <= Decided)
        for pid, dec in curr_decisions.items():
            prop = curr_proposals.get(pid, set())
            if not prop.issubset(dec):
                 missing = prop - dec
                 print(f"  [FAIL] Self-Inclusion (PID {pid}): Proposal {prop} not in decision {dec}. Missing {missing}")
                 all_passed = False

    if all_passed:
        print("\nSUCCESS: All checks passed!")
    else:
        print("\nFAILURE: Some checks failed.")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 validate_la.py <config_dir> <output_dir> [num_processes]")
        sys.exit(1)
        
    c_dir = sys.argv[1]
    o_dir = sys.argv[2]
    n_proc = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    
    validate(c_dir, o_dir, n_proc)
