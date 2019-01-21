import cassiopeia
import json

settingsFile = open('cassioSettings.json', 'r')
json = json.load(settingsFile)
cassiopeia.apply_settings(json)

me = cassiopeia.Summoner(name="Kalturi", region="NA")
match = me.match_history[0]
champion_played = match.participants[me].champion
role = match.participants[me].role

print(champion_played)
print(role)
