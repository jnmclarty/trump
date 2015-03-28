# -*- coding: utf-8 -*-
"""
creates mixins for munging templates
"""
###############################################################################
#
# PyLint tests that will never be applied in this module

# Too few public methods, ignored, because these are mixins
# pylint: disable-msg=R0903

# Attribute...defined outside __init__, ignored, because these are mixins
# pylint: disable-msg=W0201

pab = 'pandas_attribute_based'

class mixin_pab(object):
    """mixin for implementing attribute based pandas functions.
    Eg. DataFrame.pct_change(...)"""

    def bld_abs(self):
        """implement's pandas .abs()"""
        self.abs = {'mtype': pab, 'kwargs': {}}

    def bld_pct_change(self, **kwargs):
        """implement's pandas .pct_change()"""
        self.pct_change = {'mtype': pab, 'kwargs': kwargs}

    def bld_add(self, **kwargs):
        """implement's pandas .add()"""
        self.add = {'mtype': pab, 'kwargs': kwargs}

    def _bld_pab_generic(self, funcname, **kwargs):
        """
        implement's a generic version of an attribute based pandas function
        """
        margs = {'mtype': pab, 'kwargs': kwargs}
        self.__setattr__(funcname, margs)

pnab = 'pandas_nonattribute_based'

class mixin_pnab(object):
    """mixin for implementing non-attribute based pandas functions.
    Eg. pd.rolling_mean(DataFrame,...)"""
    def bld_rolling_mean(self, **kwargs):
        """implement's pandas rolling_mean()"""
        self.rolling_mean = {'mtype': pnab, 'kwargs': kwargs}

    def _bld_pnab_generic(self, funcname, **kwargs):
        """
        implement's a generic version of a non-attribute based pandas function
        """
        margs = {'mtype': pnab, 'kwargs': kwargs}
        self.__setattr__(funcname, margs)
