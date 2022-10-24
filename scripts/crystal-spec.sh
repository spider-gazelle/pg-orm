#!/usr/bin/env bash

PURPLE='\033[0;35m'
GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${PURPLE}starting${NC} \`crystal spec ${@}\`\n"
crystal spec --error-trace ${@} && echo -e "\n${GREEN}done${NC}\n"