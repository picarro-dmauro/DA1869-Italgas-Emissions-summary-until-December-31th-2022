# Copyright (c) 2015 Picarro, Inc. All rights reserved
"""Cursor Factory"""
import pymssql

from contextlib import contextmanager


class CursorFactory(object):
    """Class for generating cursors"""

    def __init__(self, server, user, password, database, tds_version):
        self.server = server
        self.user = user
        self.password = password
        self.database = database
        self.tds_version = tds_version
        self.cur = None

    @contextmanager
    def get_cursor_pymssql(self, as_dict=False):
        con = None
        if self.cur is not None:
            raise RuntimeError("Reentrant calls to the cursor factory are not supported.")

        try:
            con = pymssql.connect(
                self.server, self.user, self.password, self.database, tds_version=self.tds_version)
            self.cur = con.cursor(as_dict=as_dict)
            yield self.cur
        except Exception:
            if con is not None:
                con.rollback()
            raise
        else:
            con.commit()
        finally:
            if self.cur is not None:
                self.cur.close()
                self.cur = None
            if con is not None:
                con.close()

    @contextmanager
    def get_connection_pymssql(self):
        con = None
        try:
            con = pymssql.connect(self.server, self.user, self.password, self.database)
            yield con
        except Exception:
            if con is not None:
                con.rollback()
            raise
        else:
            con.commit()
        finally:
            if con is not None:
                con.close()