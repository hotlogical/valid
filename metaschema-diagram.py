import erdantic as erd
from generate_metaschema import CaspianSchema

# Easy one-liner
erd.draw(CaspianSchema, out="diagram.png")

## Or create a diagram object that you can inspect and do stuff with
#diagram = erd.create(Party)
#diagram.models
##> [PydanticModel(Adventurer), PydanticModel(Party), PydanticModel(Quest), PydanticModel(QuestGiver)]
#diagram.draw("diagram.png")