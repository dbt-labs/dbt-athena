# seeds/my_seed.csv
my_seed_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
""".lstrip()

my_seed_yaml = """
version: 2
seeds:
  - name: my_seed
    columns:
      - name: name
        tests:
          - accepted_values:
              values: ['nomatch']
              config:
                severity: error
                error_if: "!=4"
                warn_if: "!=4"

"""
