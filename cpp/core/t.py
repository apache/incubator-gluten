#!/usr/bin/env python3
import re

def convert_symbol_map_to_macos(map_file, output_file):
    """Convert GNU ld version script to macOS exported_symbols_list"""
    
    with open(map_file, 'r') as f:
        content = f.read()
    
    # Initialize symbol lists
    exported_patterns = []
    
    # Find global section
    global_match = re.search(r'global:\s*(.*?)\s*local:', content, re.DOTALL)
    if global_match:
        global_section = global_match.group(1)
        
        # Extract C++ patterns
        cpp_match = re.search(r'extern "C\+\+"\s*\{(.*?)\}', global_section, re.DOTALL)
        if cpp_match:
            cpp_content = cpp_match.group(1)
            for line in cpp_content.split('\n'):
                line = line.strip().rstrip(';')
                if not line or line.startswith('#'):
                    continue
                
                # Convert patterns like "*protobuf::*" to macOS mangled form
                if line.startswith('*'):
                    # Pattern: *protobuf::*
                    namespace = line[1:].split('::')[0]
                    # Convert to mangled C++ names
                    mangled = mangle_namespace(namespace)
                    exported_patterns.append(mangled + '*')
                elif '::*' in line:
                    # Pattern: namespace::*
                    namespace = line.split('::')[0]
                    mangled = mangle_namespace(namespace)
                    exported_patterns.append(mangled + '*')
    
    # Write macOS symbol file
    with open(output_file, 'w') as f:
        f.write("# Exported symbols for macOS\n")
        f.write("# Generated from " + map_file + "\n\n")
        
        for pattern in exported_patterns:
            f.write(pattern + '\n')
        
        # Add note about what's hidden
        f.write("\n# Note: The following are hidden (local in GNU ld):\n")
        f.write("# *google::* (except protobuf)\n")
        f.write("# glog and gflags symbols\n")

def mangle_namespace(namespace):
    """Convert namespace to mangled C++ name (simplified)"""
    # This is a simplified mangling - real mangling is more complex
    mangled = "_ZN" + str(len(namespace)) + namespace
    return mangled

if __name__ == "__main__":
    convert_symbol_map_to_macos("symbols.map", "symbols.txt")
