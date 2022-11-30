user: hypercorn user --reload --debug --bind user.local.gd:$PORT --access-logfile - --error-logfile - --log-level DEBUG
# game: hypercorn game --reload --debug --bind game.local.gd:$PORT --access-logfile - --error-logfile - --log-level DEBUG

primary:./bin/litefs -config ./etc/primary.yml
secondary:./bin/litefs -config ./etc/secondary.yml
third:./bin/litefs -config ./etc/third.yml
