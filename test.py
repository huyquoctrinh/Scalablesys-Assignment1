from datetime import timedelta
from CEP import CEP
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator, PrimitiveEventStructure
from condition.BaseRelationCondition import SmallerThanCondition, Variable
from condition.CompositeCondition import AndCondition
from stream.FileStream import FileInputStream, FileOutputStream
from plugin.stocks.Stocks import MetastockDataFormatter

googleAscendPattern = Pattern(
    SeqOperator(
        PrimitiveEventStructure("GOOG", "a"),
        PrimitiveEventStructure("GOOG", "b"),
        PrimitiveEventStructure("GOOG", "c"),
    ),
    AndCondition(
        SmallerThanCondition(Variable("a", lambda x: x["Peak Price"]),
                             Variable("b", lambda x: x["Peak Price"])),
        SmallerThanCondition(Variable("b", lambda x: x["Peak Price"]),
                             Variable("c", lambda x: x["Peak Price"]))
    ),
    timedelta(minutes=3)
)

cep = CEP([googleAscendPattern])
events = FileInputStream("test/EventFiles/NASDAQ_SHORT.txt")
cep.run(events, FileOutputStream('test/Matches', 'output.txt'), MetastockDataFormatter())
