package io.glutenproject.utils

import io.glutenproject.vectorized.JniLibLoader

trait VeloxDllLoader{
    def loadLib(loader: JniLibLoader) : Unit = {}
}

