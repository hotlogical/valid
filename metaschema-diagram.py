import erdantic as erd
from metadata_definition import MetadataDefinition

# Easy one-liner
erd.draw(MetadataDefinition, out="diagram.png")

## Or create a diagram object that you can inspect and do stuff with
#diagram = erd.create(Party)
#diagram.models
##> [PydanticModel(Adventurer), PydanticModel(Party), PydanticModel(Quest), PydanticModel(QuestGiver)]
#diagram.draw("diagram.png")