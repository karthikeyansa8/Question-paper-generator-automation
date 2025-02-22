def get_qp_varaint_by_roll_number(roll_number):

    roll_number = int(roll_number[1:])

    remainder  = roll_number%4

    if remainder == 1:
        return 'A'
    
    elif remainder == 2:
        return 'B'

    elif remainder == 3:
        return 'C'
    
    elif remainder == 0:
        return 'D'

if __name__ == "__main__":

    roll_number = 'D1004'

    qp = get_qp_varaint_by_roll_number(roll_number)

    print(qp)