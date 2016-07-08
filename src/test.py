import rolling_median

def test_case_0():
    '''
    Plain vanilla... test
    '''
    rolling_median.process('venmo_input/venmo-trans-0.txt', \
                   'venmo_output/output-0.txt')

def test_case_1():
    '''
    Handling of payment expiry records.
    '''
    rolling_median.process('venmo_input/venmo-trans-1.txt', \
                   'venmo_output/output-1.txt')
def test_case_2():
    '''
    Handling of payment expiry, out of order sequence and disjoint graphs
    '''
    rolling_median.process('venmo_input/venmo-trans-2.txt', \
                   'venmo_output/output-2.txt')

def test_case_large():
    '''
    Handling of decent load of payment records
    '''
    rolling_median.process('venmo_input/venmo-trans.txt', \
                   'venmo_output/output.txt')



if __name__ == '__main__':
    test_case_0()
    test_case_1()
    test_case_2()
    test_case_large()
