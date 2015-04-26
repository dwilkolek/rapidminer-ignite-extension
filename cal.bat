cls
call gradlew.bat clean
call gradlew.bat


echo Delete in ignite
del "..\rapidminer-node\ignite\libs\ext\rapidminer-ignite-extension-1.0.0-all.jar"
echo Delete in rapidminer
del "..\rapidminer-node\rapidminer\lib\plugins\rapidminer-ignite-extension-1.0.0-all.jar"



echo Copy to node ignite-fabric 
copy "..\rapidminer\lib\plugins\rapidminer-ignite-extension-1.0.0-all.jar" "../rapidminer-node/ignite/libs/ext/" /Y

echo Copy to node rapidminer 
copy "..\rapidminer\lib\plugins\rapidminer-ignite-extension-1.0.0-all.jar" "../rapidminer-node/rapidminer/lib/plugins/" /Y

rem call ..\rapidminer-node\ignite-fabric\bin\ignite.bat


rem copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "C:/dev/magisterka/mgr/node/rapidminer/lib/plugins/" /Y
rem copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "C:/dev/RapidMiner_Vega/0/lib/plugins/" /Y
rem copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "C:/dev/RapidMiner_Vega/1/lib/plugins/" /Y
rem copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "C:/dev/RapidMiner_Vega/2/lib/plugins/" /Y
rem copy "..\rapidminer\lib\plugins\rapidminer-ggextension-1.0.0-all.jar" "C:/dev/RapidMiner_Vega/3/lib/plugins/" /Y

rem echo Copy ignite-fabric to rapidminer 
rem copy "..\rapidminer\lib\plugins\rapidminer-ignite-extension-1.0.0-all.jar" "../libs/ignite-fabric/libs/ext/" /Y

