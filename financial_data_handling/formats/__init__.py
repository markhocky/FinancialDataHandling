

class StorageResource():

    def select_folder(self, store):
        raise NotImplementedError

    def filename(self):
        raise NotImplementedError

    def load_from(self, file_path):
        raise NotImplementedError

    def save_to(self, file_path):
        raise NotImplementedError

