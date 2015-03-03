# -*- coding: utf-8 -*-

def PadTwo(i):
    """
    Turns an integer into '00#'
    """
    if i < 10:
        return "00" + str(i)
    else:
        return "0" + str(i)

def vp(i):
    """
    Turns an integer into a string of the form 'vp_00#'
    """
    return "vp_" + PadTwo(i)
    
def xvp(vp):
    """
    Turns a string of the form 'vp_00#' to an integer, #
    """
    return int(vp[3:])