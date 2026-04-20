class DiagnosticState:
    def __init__(self):
        self.total_pass_checks = 0
        self.total_warning_checks = 0
        self.total_fail_checks = 0
        self.total_unknown_error_checks = 0

    def add_result(self, result: str):
        if result == 'PASS':
            self.total_pass_checks += 1
        elif result == 'WARNING':
            self.total_warning_checks += 1
        elif result == 'FAIL':
            self.total_fail_checks += 1
        else:
            self.total_unknown_error_checks += 1

    def to_list(self):
        return [
            ['Pass', self.total_pass_checks],
            ['Fail', self.total_fail_checks],
            ['Warning', self.total_warning_checks],
            ['UnknownError', self.total_unknown_error_checks]
        ]
