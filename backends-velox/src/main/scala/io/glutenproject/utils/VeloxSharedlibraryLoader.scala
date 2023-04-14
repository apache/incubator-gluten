package io.glutenproject.utils

import io.glutenproject.vectorized.JniLibLoader

trait VeloxSharedlibraryLoader{
    def loadLib(loader: JniLibLoader) : Unit = {}
}

