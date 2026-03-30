################################################################################
# CMakeLists.txt
#
# Copyright (C) 2026, Gantner Instruments GmbH. All rights reserved.
################################################################################
cmake_minimum_required(VERSION 3.19)

#-------------------------------------------------------------------------------
# Project
#-------------------------------------------------------------------------------
project(pygidata LANGUAGES NONE)

set(GINS_PYGIDATA_INSTALL_SUFFIX "appdata/api/Examples/Python/ginsutility"
    CACHE PATH "Allow to change the default installation suffix.")

option(GINS_PYGIDATA_INSTALL_EXAMPLES "Enable to install the ginsutility examples" ON)

if(GINS_PYGIDATA_INSTALL_EXAMPLES)
    install(DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/src/ginsutility/examples/
        DESTINATION ${GINS_PYGIDATA_INSTALL_SUFFIX}
        COMPONENT example
        PATTERN "*__pycache__*" EXCLUDE
    )

    install(FILES ${CMAKE_CURRENT_LIST_DIR}/README.md
        DESTINATION ${GINS_PYGIDATA_INSTALL_SUFFIX}
        COMPONENT example
    )
endif()


