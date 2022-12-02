from .stottrs import Mapping
try:
    import rdflib
    from .functions import to_graph
except:
    logging.debug("RDFLib not found, install it to use the function to_graph")
