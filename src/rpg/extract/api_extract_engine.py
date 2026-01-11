from rpg.extract.extract_engine_base import ExtractEngineBase


class ApiExtractEngine(ExtractEngineBase):

    def extract_inventory(self):
        raise NotImplementedError

    def extract_reservations(self):
        raise NotImplementedError